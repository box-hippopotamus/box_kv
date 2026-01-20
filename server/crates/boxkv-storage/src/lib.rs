mod async_fs;
mod error;
mod sync_fs;

// 错误类型
pub use error::StorageError;

// 同步接口（用于后台任务）
pub use sync_fs::{
    FileSystem, LocalFileSystem, RandomAccessFileReader, ReadableFile, WritableFile,
    WritableFileWriter,
};

// 异步接口（用于服务层高并发）
pub use async_fs::{
    AsyncFileSystem, AsyncLocalFileSystem, AsyncReadableFile, AsyncWritableFile,
    AsyncWritableFileWriter,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// 测试基础文件读写操作
    #[test]
    fn test_basic_file_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_basic.txt");

        // 测试写入
        {
            let mut writer = fs
                .open_write(&file_path)
                .expect("Failed to open file for writing");
            writer.write(b"hello world").expect("Failed to write data");
            writer.flush().expect("Failed to flush");
            writer.sync().expect("Failed to sync");
            writer.close().expect("Failed to close");
        }

        // 验证文件存在
        assert!(fs.exists(&file_path));
        assert_eq!(fs.file_size(&file_path).unwrap(), 11);

        // 测试读取
        {
            let reader = fs
                .open_read(&file_path)
                .expect("Failed to open file for reading");
            let data = reader.read_all().expect("Failed to read data");
            assert_eq!(data.as_ref(), b"hello world");
        }
    }

    /// 测试 WritableFileWriter
    #[test]
    fn test_writable_file_writer() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_writer.txt");

        // 使用 WritableFileWriter
        {
            let file = fs.open_write(&file_path).expect("Failed to open file");
            let mut writer = WritableFileWriter::new(file, Some(64 * 1024));

            // 写入数据
            writer.append(b"hello").expect("Failed to append");
            writer.append(b" ").expect("Failed to append");
            writer.append(b"world").expect("Failed to append");

            // 检查文件大小（包括未 flush 的数据）
            assert_eq!(writer.get_file_size(), 11);

            // Flush 缓冲区
            writer.flush().expect("Failed to flush");

            // 同步到磁盘
            writer.sync().expect("Failed to sync");

            // 关闭文件
            writer.close().expect("Failed to close");
        }

        // 验证文件内容
        {
            let reader = fs
                .open_read(&file_path)
                .expect("Failed to open file for reading");
            let data = reader.read_all().expect("Failed to read data");
            assert_eq!(data.as_ref(), b"hello world");
        }
    }

    /// 测试 WritableFileWriter 的 offset 跟踪
    #[test]
    fn test_writable_file_writer_offset_tracking() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_offset.txt");

        let expected_size = {
            let file = fs.open_write(&file_path).expect("Failed to open file");
            let mut writer = WritableFileWriter::new(file, Some(64 * 1024));

            // 写入多个块
            for i in 0..10 {
                let data = format!("block_{}\n", i);
                writer.append(data.as_bytes()).expect("Failed to append");
            }

            // 文件大小应该是所有数据的总和
            let size = (0..10)
                .map(|i| format!("block_{}\n", i).len())
                .sum::<usize>() as u64;
            assert_eq!(writer.get_file_size(), size);

            writer.close().expect("Failed to close");
            size
        };

        // 验证文件大小
        assert_eq!(fs.file_size(&file_path).unwrap(), expected_size);
    }

    /// 测试 RandomAccessFileReader
    #[test]
    fn test_random_access_file_reader() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_reader.txt");

        // 写入测试数据
        {
            let file = fs.open_write(&file_path).expect("Failed to open file");
            let mut writer = WritableFileWriter::new(file, Some(64 * 1024));
            writer
                .append(b"0123456789abcdefghijklmnopqrstuvwxyz")
                .expect("Failed to append");
            writer.close().expect("Failed to close");
        }

        // 使用 RandomAccessFileReader 读取
        {
            let file = fs.open_read(&file_path).expect("Failed to open file");
            let reader = RandomAccessFileReader::new(file);

            // 从不同位置读取
            let data = reader.read(0, 5).expect("Failed to read");
            assert_eq!(data.as_ref(), b"01234");

            let data = reader.read(10, 5).expect("Failed to read");
            assert_eq!(data.as_ref(), b"abcde");

            let data = reader.read(30, 6).expect("Failed to read");
            assert_eq!(data.as_ref(), b"uvwxyz");
        }

        // 验证文件大小
        assert_eq!(fs.file_size(&file_path).unwrap(), 36);
    }

    /// 测试随机位置读取 (read_at)
    #[test]
    fn test_read_at_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_read_at.txt");

        // 写入测试数据
        let test_data = b"0123456789abcdefghijklmnopqrstuvwxyz";
        {
            let mut writer = fs
                .open_write(&file_path)
                .expect("Failed to open file for writing");
            writer.write(test_data).expect("Failed to write data");
            writer.sync().expect("Failed to sync");
            writer.close().expect("Failed to close");
        }

        // 测试不同位置的读取
        {
            let reader = fs
                .open_read(&file_path)
                .expect("Failed to open file for reading");

            // 从开头读取
            let data = reader.read_at(0, 5).expect("Failed to read from offset 0");
            assert_eq!(data.as_ref(), b"01234");

            // 从中间读取
            let data = reader
                .read_at(10, 5)
                .expect("Failed to read from offset 10");
            assert_eq!(data.as_ref(), b"abcde");

            // 从末尾读取
            let data = reader
                .read_at(30, 6)
                .expect("Failed to read from offset 30");
            assert_eq!(data.as_ref(), b"uvwxyz");
        }
    }

    /// 测试大文件操作
    #[test]
    fn test_large_file_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_large.txt");

        // 创建1MB的测试数据
        let chunk_size = 1024;
        let num_chunks = 1024;
        let test_chunk: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();

        // 写入大文件
        {
            let mut writer = fs
                .open_write(&file_path)
                .expect("Failed to open file for writing");
            for _ in 0..num_chunks {
                writer.write(&test_chunk).expect("Failed to write chunk");
            }
            writer.sync().expect("Failed to sync");
            writer.close().expect("Failed to close");
        }

        // 验证文件大小
        let expected_size = (chunk_size * num_chunks) as u64;
        assert_eq!(fs.file_size(&file_path).unwrap(), expected_size);

        // 随机读取验证
        {
            let reader = fs
                .open_read(&file_path)
                .expect("Failed to open file for reading");

            // 读取第一个chunk
            let data = reader
                .read_at(0, chunk_size)
                .expect("Failed to read first chunk");
            assert_eq!(data.as_ref(), &test_chunk);

            // 读取中间的chunk
            let middle_offset = (chunk_size * 512) as u64;
            let data = reader
                .read_at(middle_offset, chunk_size)
                .expect("Failed to read middle chunk");
            assert_eq!(data.as_ref(), &test_chunk);

            // 读取最后的chunk
            let last_offset = (chunk_size * (num_chunks - 1)) as u64;
            let data = reader
                .read_at(last_offset, chunk_size)
                .expect("Failed to read last chunk");
            assert_eq!(data.as_ref(), &test_chunk);
        }
    }

    /// 测试目录操作
    #[test]
    fn test_directory_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;

        // 测试创建目录
        let sub_dir = dir.path().join("subdir").join("nested");
        fs.create_dir(&sub_dir).expect("Failed to create directory");
        assert!(sub_dir.exists());

        // 在目录中创建文件
        let file1 = sub_dir.join("file1.txt");
        let file2 = sub_dir.join("file2.txt");

        {
            let mut writer = fs.open_write(&file1).expect("Failed to create file1");
            writer.write(b"content1").expect("Failed to write to file1");
            writer.close().expect("Failed to close file1");
        }

        {
            let mut writer = fs.open_write(&file2).expect("Failed to create file2");
            writer.write(b"content2").expect("Failed to write to file2");
            writer.close().expect("Failed to close file2");
        }

        // 测试列出目录内容
        let mut entries = fs.list_dir(&sub_dir).expect("Failed to list directory");
        entries.sort();
        assert_eq!(entries, vec!["file1.txt", "file2.txt"]);

        // 测试删除文件
        fs.delete(&file1).expect("Failed to delete file1");
        assert!(!fs.exists(&file1));
        assert!(fs.exists(&file2));

        let entries = fs
            .list_dir(&sub_dir)
            .expect("Failed to list directory after deletion");
        assert_eq!(entries, vec!["file2.txt"]);
    }

    /// 测试错误处理
    #[test]
    fn test_error_handling() {
        let fs = LocalFileSystem;

        // 测试读取不存在的文件
        let non_existent = PathBuf::from("/non/existent/file.txt");
        let result = fs.open_read(&non_existent);
        assert!(result.is_err());

        // 测试删除不存在的文件
        let result = fs.delete(&non_existent);
        assert!(result.is_err());

        // 测试获取不存在文件的大小
        let result = fs.file_size(&non_existent);
        assert!(result.is_err());

        // 测试列出不存在的目录
        let result = fs.list_dir(&non_existent);
        assert!(result.is_err());

        // 测试exists对不存在文件返回false
        assert!(!fs.exists(&non_existent));
    }

    /// 测试文件截断行为
    #[test]
    fn test_file_truncation() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_truncate.txt");

        // 第一次写入
        {
            let mut writer = fs
                .open_write(&file_path)
                .expect("Failed to open file for writing");
            writer
                .write(b"original content that is quite long")
                .expect("Failed to write data");
            writer.close().expect("Failed to close");
        }

        let original_size = fs.file_size(&file_path).unwrap();
        assert!(original_size > 10);

        // 第二次写入（应该截断）
        {
            let mut writer = fs
                .open_write(&file_path)
                .expect("Failed to open file for writing");
            writer.write(b"short").expect("Failed to write data");
            writer.close().expect("Failed to close");
        }

        // 验证文件被截断
        assert_eq!(fs.file_size(&file_path).unwrap(), 5);

        {
            let reader = fs
                .open_read(&file_path)
                .expect("Failed to open file for reading");
            let data = reader.read_all().expect("Failed to read data");
            assert_eq!(data.as_ref(), b"short");
        }
    }

    /// 测试并发安全性（基础测试）
    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().expect("Failed to create temp dir");
        let fs = Arc::new(LocalFileSystem);
        let base_path = Arc::new(dir.path().to_path_buf());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let fs = Arc::clone(&fs);
                let base_path = Arc::clone(&base_path);

                thread::spawn(move || {
                    let file_path = base_path.join(format!("concurrent_{}.txt", i));
                    let data = format!("data from thread {}", i);

                    // 写入
                    {
                        let mut writer = fs
                            .open_write(&file_path)
                            .expect("Failed to open file for writing");
                        writer.write(data.as_bytes()).expect("Failed to write data");
                        writer.sync().expect("Failed to sync");
                        writer.close().expect("Failed to close");
                    }

                    // 读取验证
                    {
                        let reader = fs
                            .open_read(&file_path)
                            .expect("Failed to open file for reading");
                        let read_data = reader.read_all().expect("Failed to read data");
                        assert_eq!(read_data.as_ref(), data.as_bytes());
                    }

                    i
                })
            })
            .collect();

        // 等待所有线程完成
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // 验证所有文件都存在
        for i in 0..10 {
            let file_path = base_path.join(format!("concurrent_{}.txt", i));
            assert!(fs.exists(&file_path));
        }
    }

    /// 测试边界条件
    #[test]
    fn test_edge_cases() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;

        // 测试空文件
        let empty_file = dir.path().join("empty.txt");
        {
            let writer = fs
                .open_write(&empty_file)
                .expect("Failed to create empty file");
            writer.close().expect("Failed to close empty file");
        }

        assert_eq!(fs.file_size(&empty_file).unwrap(), 0);

        {
            let reader = fs
                .open_read(&empty_file)
                .expect("Failed to open empty file");
            let data = reader.read_all().expect("Failed to read empty file");
            assert!(data.is_empty());
        }

        // 测试只写入一个字节
        let single_byte_file = dir.path().join("single_byte.txt");
        {
            let mut writer = fs
                .open_write(&single_byte_file)
                .expect("Failed to create single byte file");
            writer.write(&[42]).expect("Failed to write single byte");
            writer.close().expect("Failed to close single byte file");
        }

        assert_eq!(fs.file_size(&single_byte_file).unwrap(), 1);

        {
            let reader = fs
                .open_read(&single_byte_file)
                .expect("Failed to open single byte file");
            let data = reader.read_at(0, 1).expect("Failed to read single byte");
            assert_eq!(data.as_ref(), &[42]);
        }
    }

    /// 性能基准测试（简化版）
    #[test]
    fn test_performance_baseline() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("perf_test.txt");

        let start = std::time::Instant::now();

        // 写入1000个小块
        {
            let mut writer = fs
                .open_write(&file_path)
                .expect("Failed to open file for writing");
            for i in 0..1000 {
                let data = format!("block_{:04}\n", i);
                writer
                    .write(data.as_bytes())
                    .expect("Failed to write block");
            }
            writer.sync().expect("Failed to sync");
            writer.close().expect("Failed to close");
        }

        let write_duration = start.elapsed();

        // 随机读取测试
        let read_start = std::time::Instant::now();
        {
            let reader = fs
                .open_read(&file_path)
                .expect("Failed to open file for reading");

            // 读取前10个块
            for i in 0..10 {
                let offset = (i * 11) as u64; // 每个块大约11字节
                let _data = reader.read_at(offset, 11).expect("Failed to read block");
            }
        }
        let read_duration = read_start.elapsed();

        // 简单的性能断言（这些值应该根据实际环境调整）
        assert!(
            write_duration.as_millis() < 1000,
            "Write took too long: {:?}",
            write_duration
        );
        assert!(
            read_duration.as_millis() < 100,
            "Read took too long: {:?}",
            read_duration
        );

        println!(
            "Performance baseline - Write: {:?}, Read: {:?}",
            write_duration, read_duration
        );
    }

    /// 测试异步文件系统
    #[tokio::test]
    async fn test_async_file_system() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async.txt");

        // 测试异步写入
        {
            let mut writer = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file for writing");
            writer
                .write(b"hello async world")
                .await
                .expect("Failed to write data");
            writer.flush().await.expect("Failed to flush");
            writer.sync().await.expect("Failed to sync");
            writer.close().await.expect("Failed to close");
        }

        // 验证文件存在
        assert!(fs.exists(&file_path).await);
        assert_eq!(fs.file_size(&file_path).await.unwrap(), 17);

        // 测试异步读取
        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");
            let data = reader.read_all().await.expect("Failed to read data");
            assert_eq!(data.as_ref(), b"hello async world");
        }
    }

    /// 测试异步 WritableFileWriter
    #[tokio::test]
    async fn test_async_writable_file_writer() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_writer.txt");

        // 使用 AsyncWritableFileWriter
        {
            let file = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file");
            let mut writer = AsyncWritableFileWriter::new(file, Some(64 * 1024));

            // 写入数据
            writer.append(b"hello").await.expect("Failed to append");
            writer.append(b" ").await.expect("Failed to append");
            writer.append(b"async").await.expect("Failed to append");
            writer.append(b" ").await.expect("Failed to append");
            writer.append(b"world").await.expect("Failed to append");

            // 检查文件大小（包括未 flush 的数据）
            assert_eq!(writer.get_file_size(), 17);

            // Flush 缓冲区
            writer.flush().await.expect("Failed to flush");

            // 同步到磁盘
            writer.sync().await.expect("Failed to sync");

            // 关闭文件
            writer.close().await.expect("Failed to close");
        }

        // 验证文件内容
        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");
            let data = reader.read_all().await.expect("Failed to read data");
            assert_eq!(data.as_ref(), b"hello async world");
        }
    }

    /// 测试异步 WritableFileWriter 的 offset 跟踪
    #[tokio::test]
    async fn test_async_writable_file_writer_offset_tracking() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_offset.txt");

        let expected_size = {
            let file = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file");
            let mut writer = AsyncWritableFileWriter::new(file, Some(64 * 1024));

            // 写入多个块
            for i in 0..10 {
                let data = format!("block_{}\n", i);
                writer
                    .append(data.as_bytes())
                    .await
                    .expect("Failed to append");
            }

            // 文件大小应该是所有数据的总和
            let size = (0..10)
                .map(|i| format!("block_{}\n", i).len())
                .sum::<usize>() as u64;
            assert_eq!(writer.get_file_size(), size);

            writer.close().await.expect("Failed to close");
            size
        };

        // 验证文件大小
        assert_eq!(fs.file_size(&file_path).await.unwrap(), expected_size);
    }

    /// 测试异步随机位置读取 (read_at)
    #[tokio::test]
    async fn test_async_read_at_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_read_at.txt");

        // 写入测试数据
        let test_data = b"0123456789abcdefghijklmnopqrstuvwxyz";
        {
            let mut writer = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file for writing");
            writer.write(test_data).await.expect("Failed to write data");
            writer.sync().await.expect("Failed to sync");
            writer.close().await.expect("Failed to close");
        }

        // 测试不同位置的读取
        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");

            // 从开头读取
            let data = reader
                .read_at(0, 5)
                .await
                .expect("Failed to read from offset 0");
            assert_eq!(data.as_ref(), b"01234");

            // 从中间读取
            let data = reader
                .read_at(10, 5)
                .await
                .expect("Failed to read from offset 10");
            assert_eq!(data.as_ref(), b"abcde");

            // 从末尾读取
            let data = reader
                .read_at(30, 6)
                .await
                .expect("Failed to read from offset 30");
            assert_eq!(data.as_ref(), b"uvwxyz");
        }
    }

    /// 测试异步大文件操作
    #[tokio::test]
    async fn test_async_large_file_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_large.txt");

        // 创建1MB的测试数据
        let chunk_size = 1024;
        let num_chunks = 1024;
        let test_chunk: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();

        // 写入大文件
        {
            let mut writer = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file for writing");
            for _ in 0..num_chunks {
                writer
                    .write(&test_chunk)
                    .await
                    .expect("Failed to write chunk");
            }
            writer.sync().await.expect("Failed to sync");
            writer.close().await.expect("Failed to close");
        }

        // 验证文件大小
        let expected_size = (chunk_size * num_chunks) as u64;
        assert_eq!(fs.file_size(&file_path).await.unwrap(), expected_size);

        // 随机读取验证
        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");

            // 读取第一个chunk
            let data = reader
                .read_at(0, chunk_size)
                .await
                .expect("Failed to read first chunk");
            assert_eq!(data.as_ref(), &test_chunk);

            // 读取中间的chunk
            let middle_offset = (chunk_size * 512) as u64;
            let data = reader
                .read_at(middle_offset, chunk_size)
                .await
                .expect("Failed to read middle chunk");
            assert_eq!(data.as_ref(), &test_chunk);

            // 读取最后的chunk
            let last_offset = (chunk_size * (num_chunks - 1)) as u64;
            let data = reader
                .read_at(last_offset, chunk_size)
                .await
                .expect("Failed to read last chunk");
            assert_eq!(data.as_ref(), &test_chunk);
        }
    }

    /// 测试异步目录操作
    #[tokio::test]
    async fn test_async_directory_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;

        // 测试创建目录
        let sub_dir = dir.path().join("subdir").join("nested");
        fs.create_dir(&sub_dir)
            .await
            .expect("Failed to create directory");
        assert!(sub_dir.exists());

        // 在目录中创建文件
        let file1 = sub_dir.join("file1.txt");
        let file2 = sub_dir.join("file2.txt");

        {
            let mut writer = fs.open_write(&file1).await.expect("Failed to create file1");
            writer
                .write(b"content1")
                .await
                .expect("Failed to write to file1");
            writer.close().await.expect("Failed to close file1");
        }

        {
            let mut writer = fs.open_write(&file2).await.expect("Failed to create file2");
            writer
                .write(b"content2")
                .await
                .expect("Failed to write to file2");
            writer.close().await.expect("Failed to close file2");
        }

        // 测试列出目录内容
        let mut entries = fs
            .list_dir(&sub_dir)
            .await
            .expect("Failed to list directory");
        entries.sort();
        assert_eq!(entries, vec!["file1.txt", "file2.txt"]);

        // 测试删除文件
        fs.delete(&file1).await.expect("Failed to delete file1");
        assert!(!fs.exists(&file1).await);
        assert!(fs.exists(&file2).await);

        let entries = fs
            .list_dir(&sub_dir)
            .await
            .expect("Failed to list directory after deletion");
        assert_eq!(entries, vec!["file2.txt"]);
    }

    /// 测试异步错误处理
    #[tokio::test]
    async fn test_async_error_handling() {
        let fs = AsyncLocalFileSystem;

        // 测试读取不存在的文件
        let non_existent = PathBuf::from("/non/existent/file.txt");
        let result = fs.open_read(&non_existent).await;
        assert!(result.is_err());

        // 测试删除不存在的文件
        let result = fs.delete(&non_existent).await;
        assert!(result.is_err());

        // 测试获取不存在文件的大小
        let result = fs.file_size(&non_existent).await;
        assert!(result.is_err());

        // 测试列出不存在的目录
        let result = fs.list_dir(&non_existent).await;
        assert!(result.is_err());

        // 测试exists对不存在文件返回false
        assert!(!fs.exists(&non_existent).await);
    }

    /// 测试异步文件截断行为
    #[tokio::test]
    async fn test_async_file_truncation() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_truncate.txt");

        // 第一次写入
        {
            let mut writer = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file for writing");
            writer
                .write(b"original content that is quite long")
                .await
                .expect("Failed to write data");
            writer.close().await.expect("Failed to close");
        }

        let original_size = fs.file_size(&file_path).await.unwrap();
        assert!(original_size > 10);

        // 第二次写入（应该截断）
        {
            let mut writer = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file for writing");
            writer.write(b"short").await.expect("Failed to write data");
            writer.close().await.expect("Failed to close");
        }

        // 验证文件被截断
        assert_eq!(fs.file_size(&file_path).await.unwrap(), 5);

        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");
            let data = reader.read_all().await.expect("Failed to read data");
            assert_eq!(data.as_ref(), b"short");
        }
    }

    /// 测试异步并发安全性
    #[tokio::test]
    async fn test_async_concurrent_access() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let base_path = dir.path().to_path_buf();

        // 创建多个并发任务
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let file_path = base_path.join(format!("async_concurrent_{}.txt", i));
                let data = format!("data from task {}", i);

                tokio::spawn(async move {
                    // 写入
                    {
                        let mut writer = fs
                            .open_write(&file_path)
                            .await
                            .expect("Failed to open file for writing");
                        writer
                            .write(data.as_bytes())
                            .await
                            .expect("Failed to write data");
                        writer.sync().await.expect("Failed to sync");
                        writer.close().await.expect("Failed to close");
                    }

                    // 读取验证
                    {
                        let reader = fs
                            .open_read(&file_path)
                            .await
                            .expect("Failed to open file for reading");
                        let read_data = reader.read_all().await.expect("Failed to read data");
                        assert_eq!(read_data.as_ref(), data.as_bytes());
                    }

                    i
                })
            })
            .collect();

        // 等待所有任务完成
        for handle in handles {
            handle.await.expect("Task panicked");
        }

        // 验证所有文件都存在
        for i in 0..10 {
            let file_path = base_path.join(format!("async_concurrent_{}.txt", i));
            assert!(fs.exists(&file_path).await);
        }
    }

    /// 测试异步边界条件
    #[tokio::test]
    async fn test_async_edge_cases() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;

        // 测试空文件
        let empty_file = dir.path().join("async_empty.txt");
        {
            let writer = fs
                .open_write(&empty_file)
                .await
                .expect("Failed to create empty file");
            writer.close().await.expect("Failed to close empty file");
        }

        assert_eq!(fs.file_size(&empty_file).await.unwrap(), 0);

        {
            let reader = fs
                .open_read(&empty_file)
                .await
                .expect("Failed to open empty file");
            let data = reader.read_all().await.expect("Failed to read empty file");
            assert!(data.is_empty());
        }

        // 测试只写入一个字节
        let single_byte_file = dir.path().join("async_single_byte.txt");
        {
            let mut writer = fs
                .open_write(&single_byte_file)
                .await
                .expect("Failed to create single byte file");
            writer
                .write(&[42])
                .await
                .expect("Failed to write single byte");
            writer
                .close()
                .await
                .expect("Failed to close single byte file");
        }

        assert_eq!(fs.file_size(&single_byte_file).await.unwrap(), 1);

        {
            let reader = fs
                .open_read(&single_byte_file)
                .await
                .expect("Failed to open single byte file");
            let data = reader
                .read_at(0, 1)
                .await
                .expect("Failed to read single byte");
            assert_eq!(data.as_ref(), &[42]);
        }
    }

    /// 测试异步性能基准（简化版）
    #[tokio::test]
    async fn test_async_performance_baseline() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("async_perf_test.txt");

        let start = std::time::Instant::now();

        // 写入1000个小块
        {
            let mut writer = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file for writing");
            for i in 0..1000 {
                let data = format!("block_{:04}\n", i);
                writer
                    .write(data.as_bytes())
                    .await
                    .expect("Failed to write block");
            }
            writer.sync().await.expect("Failed to sync");
            writer.close().await.expect("Failed to close");
        }

        let write_duration = start.elapsed();

        // 随机读取测试
        let read_start = std::time::Instant::now();
        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");

            // 读取前10个块
            for i in 0..10 {
                let offset = (i * 11) as u64; // 每个块大约11字节
                let _data = reader
                    .read_at(offset, 11)
                    .await
                    .expect("Failed to read block");
            }
        }
        let read_duration = read_start.elapsed();

        // 简单的性能断言（这些值应该根据实际环境调整）
        assert!(
            write_duration.as_millis() < 2000,
            "Write took too long: {:?}",
            write_duration
        );
        assert!(
            read_duration.as_millis() < 200,
            "Read took too long: {:?}",
            read_duration
        );

        println!(
            "Async performance baseline - Write: {:?}, Read: {:?}",
            write_duration, read_duration
        );
    }

    /// 测试异步 WritableFileWriter 的大数据块处理
    #[tokio::test]
    async fn test_async_writable_file_writer_large_chunks() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_large_chunks.txt");

        // 创建大于缓冲区大小的数据块
        let large_data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();

        {
            let file = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file");
            let mut writer = AsyncWritableFileWriter::new(file, Some(64 * 1024));

            // 写入大块数据（应该绕过缓冲区直接写入）
            writer
                .append(&large_data)
                .await
                .expect("Failed to append large chunk");

            assert_eq!(writer.get_file_size(), large_data.len() as u64);

            writer.flush().await.expect("Failed to flush");
            writer.sync().await.expect("Failed to sync");
            writer.close().await.expect("Failed to close");
        }

        // 验证文件内容
        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");
            let data = reader.read_all().await.expect("Failed to read data");
            assert_eq!(data.as_ref(), &large_data);
        }
    }

    /// 测试异步 prefetch（如果支持）
    #[tokio::test]
    async fn test_async_prefetch() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_prefetch.txt");

        // 写入测试数据
        let test_data = b"0123456789abcdefghijklmnopqrstuvwxyz";
        {
            let mut writer = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file for writing");
            writer.write(test_data).await.expect("Failed to write data");
            writer.close().await.expect("Failed to close");
        }

        // 测试 prefetch（应该不会出错，即使实现为空）
        {
            let reader = fs
                .open_read(&file_path)
                .await
                .expect("Failed to open file for reading");
            // prefetch 应该成功（即使不做任何操作）
            reader
                .prefetch(0, 10)
                .await
                .expect("Prefetch should not fail");
            reader
                .prefetch(10, 10)
                .await
                .expect("Prefetch should not fail");
        }
    }

    /// 测试同步 prefetch（如果支持）
    #[test]
    fn test_sync_prefetch() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_sync_prefetch.txt");

        // 写入测试数据
        let test_data = b"0123456789abcdefghijklmnopqrstuvwxyz";
        {
            let mut writer = fs
                .open_write(&file_path)
                .expect("Failed to open file for writing");
            writer.write(test_data).expect("Failed to write data");
            writer.close().expect("Failed to close");
        }

        // 测试 prefetch（应该不会出错，即使实现为空）
        {
            let reader = fs
                .open_read(&file_path)
                .expect("Failed to open file for reading");
            // prefetch 应该成功（即使不做任何操作）
            reader.prefetch(0, 10).expect("Prefetch should not fail");
            reader.prefetch(10, 10).expect("Prefetch should not fail");
        }
    }

    /// 测试 WritableFileWriter 的错误状态管理
    #[test]
    fn test_writable_file_writer_error_state() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_error_state.txt");

        let file = fs.open_write(&file_path).expect("Failed to open file");
        let mut writer = WritableFileWriter::new(file, Some(64 * 1024));

        // 正常写入
        writer.append(b"test").expect("Failed to append");

        // 关闭文件（模拟错误场景）
        // 注意：这里我们无法直接模拟底层文件错误，但可以测试错误状态检查
        assert!(!writer.seen_error());

        writer.close().expect("Failed to close");
    }

    /// 测试 AsyncWritableFileWriter 的错误状态管理
    #[tokio::test]
    async fn test_async_writable_file_writer_error_state() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_error_state.txt");

        let file = fs
            .open_write(&file_path)
            .await
            .expect("Failed to open file");
        let mut writer = AsyncWritableFileWriter::new(file, Some(64 * 1024));

        // 正常写入
        writer.append(b"test").await.expect("Failed to append");

        // 检查错误状态
        assert!(!writer.seen_error());

        writer.close().await.expect("Failed to close");
    }

    /// 测试 WritableFileWriter 的缓冲区管理
    #[test]
    fn test_writable_file_writer_buffer_management() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = LocalFileSystem;
        let file_path = dir.path().join("test_buffer_mgmt.txt");

        {
            let file = fs.open_write(&file_path).expect("Failed to open file");
            let mut writer = WritableFileWriter::new(file, Some(1024)); // 小缓冲区

            // 写入小于缓冲区的数据
            writer.append(b"small").expect("Failed to append");
            assert!(!writer.buffer_is_empty());

            // 写入大量数据，触发 flush
            let large_data = vec![0u8; 2048];
            writer
                .append(&large_data)
                .expect("Failed to append large data");

            writer.close().expect("Failed to close");
        }

        // 验证数据正确写入
        {
            let reader = fs.open_read(&file_path).expect("Failed to open file");
            let data = reader.read_all().expect("Failed to read");
            assert_eq!(data.len(), 5 + 2048);
        }
    }

    /// 测试 AsyncWritableFileWriter 的缓冲区管理
    #[tokio::test]
    async fn test_async_writable_file_writer_buffer_management() {
        let dir = tempdir().expect("Failed to create temp dir");
        let fs = AsyncLocalFileSystem;
        let file_path = dir.path().join("test_async_buffer_mgmt.txt");

        {
            let file = fs
                .open_write(&file_path)
                .await
                .expect("Failed to open file");
            let mut writer = AsyncWritableFileWriter::new(file, Some(1024)); // 小缓冲区

            // 写入小于缓冲区的数据
            writer.append(b"small").await.expect("Failed to append");
            assert!(!writer.buffer_is_empty());

            // 写入大量数据，触发 flush
            let large_data = vec![0u8; 2048];
            writer
                .append(&large_data)
                .await
                .expect("Failed to append large data");

            writer.close().await.expect("Failed to close");
        }

        // 验证数据正确写入
        {
            let reader = fs.open_read(&file_path).await.expect("Failed to open file");
            let data = reader.read_all().await.expect("Failed to read");
            assert_eq!(data.len(), 5 + 2048);
        }
    }

    /// 综合测试：同步和异步文件系统的一致性
    #[tokio::test]
    async fn test_sync_async_consistency() {
        let dir = tempdir().expect("Failed to create temp dir");
        let sync_fs = LocalFileSystem;
        let async_fs = AsyncLocalFileSystem;

        let test_data = b"consistency test data";
        let file_path = dir.path().join("consistency_test.txt");

        // 使用同步文件系统写入
        {
            let mut writer = sync_fs
                .open_write(&file_path)
                .expect("Failed to open for writing");
            writer.write(test_data).expect("Failed to write");
            writer.close().expect("Failed to close");
        }

        // 使用异步文件系统读取
        {
            let reader = async_fs
                .open_read(&file_path)
                .await
                .expect("Failed to open for reading");
            let data = reader.read_all().await.expect("Failed to read");
            assert_eq!(data.as_ref(), test_data);
        }

        // 删除文件
        sync_fs.delete(&file_path).expect("Failed to delete");

        // 使用异步文件系统写入
        {
            let mut writer = async_fs
                .open_write(&file_path)
                .await
                .expect("Failed to open for writing");
            writer.write(test_data).await.expect("Failed to write");
            writer.close().await.expect("Failed to close");
        }

        // 使用同步文件系统读取
        {
            let reader = sync_fs
                .open_read(&file_path)
                .expect("Failed to open for reading");
            let data = reader.read_all().expect("Failed to read");
            assert_eq!(data.as_ref(), test_data);
        }
    }
}
