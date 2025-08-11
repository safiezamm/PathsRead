using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;

namespace PathsReaderWinForm
{
    public partial class Form2ReadPaths : Form
    {
        static string connectionString = "Server=localhost;Database=PathsDb;Integrated Security=True;";
        private List<string> allFiles = new List<string>();
        private string selectedFolder = "";
        public Form2ReadPaths()
        {
            InitializeComponent();
            backgroundWorker.WorkerReportsProgress = true;
            backgroundWorker.DoWork += BackgroundWorker_DoWork;
            backgroundWorker.ProgressChanged += BackgroundWorker_ProgressChanged;
            backgroundWorker.RunWorkerCompleted += BackgroundWorker_RunWorkerCompleted;

            progressBar1.Minimum = 0;
            progressBar1.Value = 0;
            progressBar1.Style = ProgressBarStyle.Blocks;
        }
        private readonly BackgroundWorker backgroundWorker = new BackgroundWorker();
        private const int BulkBatchSize = 10000; // tune as needed (5k–50k)
        private const int UiReportEveryN = 500;  // throttle UI updates
        private const int SqlCommandTimeoutSec = 600;

       

        private void btnBrowse_Click(object sender, EventArgs e)
        {
            using (var dlg = new FolderBrowserDialog())
            {
                dlg.Description = "Select a folder to index";
                dlg.ShowNewFolderButton = false;
                if (dlg.ShowDialog(this) == DialogResult.OK)
                {
                    txtFolderPath.Text = dlg.SelectedPath;
                    Log($"Selected: {dlg.SelectedPath}");
                }
            }
        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            var folderPath = txtFolderPath.Text?.Trim();
            if (string.IsNullOrEmpty(folderPath) || !Directory.Exists(folderPath))
            {
                MessageBox.Show(this, "Please select a valid folder.", "Invalid folder", MessageBoxButtons.OK,
                    MessageBoxIcon.Warning);
                return;
            }

            btnStart.Enabled = false;
            progressBar1.Value = 0;
            lstLogs.Items.Clear();

            backgroundWorker.RunWorkerAsync(folderPath);
        }

        private void BackgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            string rootFolder = (string)e.Argument;

            Stopwatch sw = Stopwatch.StartNew();
            Log($"Ensuring table exists in database...");
            EnsureFileIndexTable();

            Log($"Counting files (to show percentage)...");
            long totalFiles = CountFilesFast(rootFolder);
            if (totalFiles <= 0)
            {
                // No files found or counting failed; switch to marquee
                Invoke((Action)(() =>
                {
                    progressBar1.Style = ProgressBarStyle.Marquee;
                }));
                Log($"Total files unknown or zero. Progress bar set to marquee.");
            }
            else
            {
                Invoke((Action)(() =>
                {
                    progressBar1.Style = ProgressBarStyle.Blocks;
                    progressBar1.Maximum = int.MaxValue; // scale to int range
                }));
                Log($"Total files detected: {totalFiles:N0}");
            }

            long processed = 0;
            long lastUiUpdateProcessed = 0;

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                CreateOrResetStageTable(connection);

                var dataTable = CreateStageDataTable();

                foreach (var filePath in EnumerateFilesSafe(rootFolder))
                {
                    processed++;

                    // Extract parts with maximum resilience
                    string fullPath = filePath;
                    string fileName = SafePathGet(() => Path.GetFileName(filePath));
                    string ext = SafePathGet(() => Path.GetExtension(filePath));
                    string dir = SafePathGet(() => Path.GetDirectoryName(filePath));

                    var row = dataTable.NewRow();
                    row["FullPath"] = fullPath ?? (object)DBNull.Value;
                    row["FileName"] = string.IsNullOrEmpty(fileName) ? (object)DBNull.Value : fileName;
                    row["Extension"] = string.IsNullOrEmpty(ext) ? (object)DBNull.Value : ext;
                    row["DirectoryPath"] = string.IsNullOrEmpty(dir) ? (object)DBNull.Value : dir;
                    dataTable.Rows.Add(row);

                    if (dataTable.Rows.Count >= BulkBatchSize)
                    {
                        BulkUpsertBatch(connection, dataTable);
                        dataTable.Clear();
                    }

                    if (processed - lastUiUpdateProcessed >= UiReportEveryN)
                    {
                        lastUiUpdateProcessed = processed;
                        int percent = totalFiles > 0
                            ? (int)Math.Min(100, (processed * 100.0 / totalFiles))
                            : 0;

                        backgroundWorker.ReportProgress(percent, processed);
                    }
                }

                if (dataTable.Rows.Count > 0)
                {
                    BulkUpsertBatch(connection, dataTable);
                    dataTable.Clear();
                }
            }

            sw.Stop();
            Log($"Done. Processed {processed:N0} files in {sw.Elapsed}.");
            e.Result = new Tuple<long, long>(processed, totalFiles);
        }

        private void BackgroundWorker_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            // Progress bar scaling: we map 0..100% to 0..int.MaxValue for smoothness
            if (progressBar1.Style == ProgressBarStyle.Blocks)
            {
                long scaled = (long)(e.ProgressPercentage / 100.0 * int.MaxValue);
                progressBar1.Value = (int)Math.Max(progressBar1.Minimum, Math.Min(scaled, progressBar1.Maximum));
            }

            if (e.UserState is long processed)
            {
                LogThrottled($"Processed: {processed:N0}");
            }
        }

        private void BackgroundWorker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            btnStart.Enabled = true;
            if (progressBar1.Style == ProgressBarStyle.Marquee)
            {
                progressBar1.Style = ProgressBarStyle.Blocks;
            }
            progressBar1.Value = progressBar1.Maximum;

            if (e.Error != null)
            {
                Log($"Error: {e.Error.Message}");
                MessageBox.Show(this, e.Error.ToString(), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            else if (e.Result is Tuple<long, long> r)
            {
                Log($"Summary: {r.Item1:N0} processed, total count seen {r.Item2:N0}.");
            }
        }

        // Robust, streaming, non-recursive enumeration that skips inaccessible paths
        private IEnumerable<string> EnumerateFilesSafe(string rootFolder)
        {
            var pending = new Stack<string>();
            pending.Push(rootFolder);

            while (pending.Count > 0)
            {
                string current = pending.Pop();

                IEnumerable<string> dirs = null;
                try
                {
                    dirs = Directory.EnumerateDirectories(current);
                }
                catch (UnauthorizedAccessException ex)
                {
                    Log($"Access denied (dir): {current} ({ex.Message})");
                }
                catch (PathTooLongException ex)
                {
                    Log($"Path too long (dir): {current} ({ex.Message})");
                }
                catch (IOException ex)
                {
                    Log($"IO error (dir): {current} ({ex.Message})");
                }
                catch (Exception ex)
                {
                    Log($"Error (dir): {current} ({ex.Message})");
                }

                if (dirs != null)
                {
                    foreach (var d in dirs)
                        pending.Push(d);
                }

                IEnumerable<string> files = null;
                try
                {
                    files = Directory.EnumerateFiles(current);
                }
                catch (UnauthorizedAccessException ex)
                {
                    Log($"Access denied (files): {current} ({ex.Message})");
                }
                catch (PathTooLongException ex)
                {
                    Log($"Path too long (files): {current} ({ex.Message})");
                }
                catch (IOException ex)
                {
                    Log($"IO error (files): {current} ({ex.Message})");
                }
                catch (Exception ex)
                {
                    Log($"Error (files): {current} ({ex.Message})");
                }

                if (files != null)
                {
                    foreach (var f in files)
                        yield return f;
                }
            }
        }

        // Counting pass for percent; catches exceptions like the main enumerator
        private long CountFilesFast(string rootFolder)
        {
            long count = 0;
            try
            {
                foreach (var _ in EnumerateFilesSafe(rootFolder))
                {
                    count++;
                    // Optional: throttle counting to keep UI responsive on very large trees
                    if (count % 1_000_000 == 0)
                        Log($"Counted {count:N0} files so far...");
                }
            }
            catch (Exception ex)
            {
                Log($"Counting error: {ex.Message}");
            }
            return count;
        }

        private static DataTable CreateStageDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("FullPath", typeof(string));
            dt.Columns.Add("FileName", typeof(string));
            dt.Columns.Add("Extension", typeof(string));
            dt.Columns.Add("DirectoryPath", typeof(string));
            return dt;
        }

        private void EnsureFileIndexTable()
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandTimeout = SqlCommandTimeoutSec;
                cmd.CommandText = @"
IF OBJECT_ID(N'dbo.FileIndex', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FileIndex(
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        FullPath NVARCHAR(4000) NOT NULL UNIQUE,
        FileName NVARCHAR(4000) NULL,
        Extension NVARCHAR(4000) NULL,
        DirectoryPath NVARCHAR(4000) NULL,
        Processd INT NOT NULL CONSTRAINT DF_FileIndex_Processd DEFAULT (0), -- 0=not process, 1=processed, 2=duplicated, 3=error
        status NVARCHAR(4000) NULL,
        CreatedAt DATETIME NOT NULL CONSTRAINT DF_FileIndex_CreatedAt DEFAULT (GETDATE())
    );
END;
";
                cmd.ExecuteNonQuery();
            }
        }

        private void CreateOrResetStageTable(SqlConnection conn)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandTimeout = SqlCommandTimeoutSec;
                cmd.CommandText = @"
IF OBJECT_ID('tempdb..#StageFileIndex') IS NOT NULL DROP TABLE #StageFileIndex;
CREATE TABLE #StageFileIndex(
    FullPath NVARCHAR(4000) NOT NULL,
    FileName NVARCHAR(4000) NULL,
    Extension NVARCHAR(4000) NULL,
    DirectoryPath NVARCHAR(4000) NULL
);
";
                cmd.ExecuteNonQuery();
            }
        }

        private void BulkUpsertBatch(SqlConnection conn, DataTable batch)
        {
            // 1) Bulk copy into temp table
            using (var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null))
            {
                bulk.DestinationTableName = "#StageFileIndex";
                bulk.BatchSize = batch.Rows.Count;
                bulk.BulkCopyTimeout = SqlCommandTimeoutSec;
                bulk.ColumnMappings.Add("FullPath", "FullPath");
                bulk.ColumnMappings.Add("FileName", "FileName");
                bulk.ColumnMappings.Add("Extension", "Extension");
                bulk.ColumnMappings.Add("DirectoryPath", "DirectoryPath");
                bulk.WriteToServer(batch);
            }

            // 2) Upsert (insert only new) with MERGE to avoid race with unique key
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandTimeout = SqlCommandTimeoutSec;
                cmd.CommandText = @"
MERGE dbo.FileIndex WITH (HOLDLOCK) AS target
USING #StageFileIndex AS source
    ON target.FullPath = source.FullPath
WHEN NOT MATCHED BY TARGET THEN
    INSERT (FullPath, FileName, Extension, DirectoryPath)
    VALUES (source.FullPath, source.FileName, source.Extension, source.DirectoryPath);
TRUNCATE TABLE #StageFileIndex;";
                cmd.ExecuteNonQuery();
            }
        }

        private string SafePathGet(Func<string> getter)
        {
            try { return getter() ?? string.Empty; }
            catch { return string.Empty; }
        }

        // Logging helpers (thread-safe)
        private readonly object _logLock = new object();
        private DateTime _lastThrottled = DateTime.MinValue;

        private void Log(string message)
        {
            try
            {
                if (InvokeRequired)
                {
                    BeginInvoke((Action)(() => AddLog(message)));
                }
                else
                {
                    AddLog(message);
                }
            }
            catch { /* no-op */ }
        }

        private void LogThrottled(string message, int minMs = 250)
        {
            lock (_logLock)
            {
                var now = DateTime.UtcNow;
                if ((now - _lastThrottled).TotalMilliseconds < minMs) return;
                _lastThrottled = now;
            }
            Log(message);
        }

        private void AddLog(string message)
        {
            // Limit list size to avoid memory growth
            if (lstLogs.Items.Count > 5000)
            {
                lstLogs.Items.RemoveAt(0);
            }
            lstLogs.Items.Add($"{DateTime.Now:HH:mm:ss} - {message}");
            lstLogs.TopIndex = lstLogs.Items.Count - 1;
        }


        private class FileIndexRow
        {
            public string FullPath { get; set; }
            public string FileName { get; set; }
            public string Extension { get; set; }
            public string DirectoryPath { get; set; }
        }

        
    }
}
