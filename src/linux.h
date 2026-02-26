#ifndef LINUX_H
#define LINUX_H

#define _GNU_SOURCE
#include "std.h"
#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <sys/syscall.h>
#include <unistd.h>

#define PROGRAM_EXIT_ERROR 1
#define PROGRAM_EXIT_OK 0

#define LINUX_PATH_MAX PATH_MAX

#define LINUX_OPEN_DIRECT O_DIRECT
#define LINUX_OPEN_READ_WRITE O_RDWR

#define LINUX_SEEK_SET SEEK_SET

typedef enum
{
  LINUX_OPEN_OK,
  LINUX_OPEN_ACCESS,
  LINUX_OPEN_BUSY,
  LINUX_OPEN_QUOTA,
  LINUX_OPEN_ALREADY_EXISTS,
  LINUX_OPEN_PATH_SEG_FAULT,
  LINUX_OPEN_PATH_FILE_TOO_BIG,
  LINUX_OPEN_INTERRUPT,
  LINUX_OPEN_FLAGS_MISUSE,
  LINUX_OPEN_IS_DIRECTORY,
  LINUX_OPEN_TOO_MANY_SYMBOLIC_LINKS,
  LINUX_OPEN_TOO_MANY_FD_OPEN,
  LINUX_OPEN_PATH_TOO_LONG,
  LINUX_OPEN_SYSTEM_OPEN_FILE_LIMIT,
  LINUX_OPEN_NO_DEVICE,
  LINUX_OPEN_NOT_FOUND,
  LINUX_OPEN_NO_MEMORY,
  LINUX_OPEN_NO_SPACE_ON_DEVICE,
  LINUX_OPEN_NOT_DIRECTORY,
  LINUX_OPEN_NO_DEVICE_OR_ADDRESS,
  LINUX_OPEN_OPERATION_NOT_SUPPORTED,
  LINUX_OPEN_PERMISSIONS,
  LINUX_OPEN_READ_ONLY_FILESYSTEM,
  LINUX_OPEN_FILE_BUSY,
  LINUX_OPEN_WOULD_BLOCK,
  LINUX_OPEN_UNKNOWN,
} LinuxOpenError;

typedef struct
{
  int fd;
  LinuxOpenError error;
} LinuxOpenResult;

LinuxOpenResult linux_open(const char *path, int flags, int mode)
{
  int fd = (int)syscall(SYS_open, path, flags, mode);

  LinuxOpenError error = LINUX_OPEN_OK;
  if (fd < 0)
  {
    switch (errno)
    {
    case EACCES:
      error = LINUX_OPEN_ACCESS;
      break;

    case EBUSY:
      error = LINUX_OPEN_BUSY;
      break;

    case EDQUOT:
      error = LINUX_OPEN_QUOTA;
      break;

    case EEXIST:
      error = LINUX_OPEN_ALREADY_EXISTS;
      break;

    case EFAULT:
      error = LINUX_OPEN_PATH_SEG_FAULT;
      break;

    case EFBIG:
    case EOVERFLOW:
      error = LINUX_OPEN_PATH_FILE_TOO_BIG;
      break;

    case EINTR:
      error = LINUX_OPEN_INTERRUPT;
      break;

    case EINVAL:
      error = LINUX_OPEN_FLAGS_MISUSE;
      break;

    case EISDIR:
      error = LINUX_OPEN_IS_DIRECTORY;
      break;

    case ELOOP:
      error = LINUX_OPEN_TOO_MANY_SYMBOLIC_LINKS;
      break;

    case EMFILE:
      error = LINUX_OPEN_TOO_MANY_FD_OPEN;
      break;

    case ENAMETOOLONG:
      error = LINUX_OPEN_PATH_TOO_LONG;
      break;

    case ENFILE:
      error = LINUX_OPEN_SYSTEM_OPEN_FILE_LIMIT;
      break;

    case ENODEV:
      error = LINUX_OPEN_NO_DEVICE;
      break;

    case ENOENT:
      error = LINUX_OPEN_NOT_FOUND;
      break;

    case ENOMEM:
      error = LINUX_OPEN_NO_MEMORY;
      break;

    case ENOSPC:
      error = LINUX_OPEN_NO_SPACE_ON_DEVICE;
      break;

    case ENOTDIR:
      error = LINUX_OPEN_NOT_DIRECTORY;
      break;

    case ENXIO:
      error = LINUX_OPEN_NO_DEVICE_OR_ADDRESS;
      break;

    case EOPNOTSUPP:
      error = LINUX_OPEN_OPERATION_NOT_SUPPORTED;
      break;

    case EPERM:
      error = LINUX_OPEN_PERMISSIONS;
      break;

    case EROFS:
      error = LINUX_OPEN_READ_ONLY_FILESYSTEM;
      break;

    case ETXTBSY:
      error = LINUX_OPEN_FILE_BUSY;
      break;

    case EWOULDBLOCK:
      error = LINUX_OPEN_WOULD_BLOCK;
      break;

    default:
      error = LINUX_OPEN_UNKNOWN;
      break;
    }
  }

  return (LinuxOpenResult){
      .fd = fd,
      .error = error,
  };
}

typedef enum
{
  LINUX_SEEK_OK,
  LINUX_SEEK_BAD_FD,
  LINUX_SEEK_WHENCE_INVALID,
  LINUX_SEEK_INVALID_OFFSET,
  LINUX_SEEK_FILE_OFFSET_TOO_BIG,
  LINUX_SEEK_NOT_A_FILE,
  LINUX_SEEK_UNKNOWN,
} LinuxSeekError;

typedef struct
{
  LinuxSeekError error;
  int64_t size;
} LinuxSeekResult;

LinuxSeekResult linux_seek(int fd, off_t offset, int whence)
{
  long result = syscall(SYS_lseek, fd, offset, whence) < 0;

  LinuxSeekError error = LINUX_SEEK_OK;
  if (result < 0)
  {
    switch (errno)
    {
    case EBADF:
      error = LINUX_SEEK_BAD_FD;
      break;

    case EINVAL:
      error = LINUX_SEEK_WHENCE_INVALID;
      break;

    case ENXIO:
      error = LINUX_SEEK_INVALID_OFFSET;
      break;

    case EOVERFLOW:
      error = LINUX_SEEK_FILE_OFFSET_TOO_BIG;
      break;

    case ESPIPE:
      error = LINUX_SEEK_NOT_A_FILE;
      break;

    default:
      error = LINUX_SEEK_UNKNOWN;
      break;
    }
  }

  return (LinuxSeekResult){
      .error = error,
      .size = result,
  };
}

typedef enum
{
  LINUX_READ_OK,
  LINUX_READ_WOULD_BLOCK,
  LINUX_READ_BAD_FD,
  LINUX_READ_BUFFER_SEG_FAULT,
  LINUX_READ_INTERRUPT,
  LINUX_READ_INVALID,
  LINUX_READ_IO,
  LINUX_READ_IS_DIR,
  LINUX_READ_UNKNOWN,
} LinuxReadError;

typedef struct
{
  size_t count;
  LinuxReadError error;
} LinuxReadResult;

LinuxReadResult linux_read(int fd, unsigned char *data, size_t length)
{
  ssize_t count = syscall(SYS_read, fd, data, length);

  LinuxReadError error = LINUX_READ_OK;
  if (count < 0)
  {
    switch (errno)
    {
    case EAGAIN:
      error = LINUX_READ_WOULD_BLOCK;
      break;

    case EBADF:
      error = LINUX_READ_BAD_FD;
      break;

    case EFAULT:
      error = LINUX_READ_BUFFER_SEG_FAULT;
      break;

    case EINTR:
      error = LINUX_READ_INTERRUPT;
      break;

    case EINVAL:
      error = LINUX_READ_INVALID;
      break;

    case EIO:
      error = LINUX_READ_IO;
      break;

    case EISDIR:
      error = LINUX_READ_IS_DIR;
      break;

    default:
      error = LINUX_READ_UNKNOWN;
      break;
    }
  }

  return (LinuxReadResult){
      .count = count,
      .error = error,
  };
}

typedef enum
{
  LINUX_WRITE_OK,
  LINUX_WRITE_WOULD_BLOCK,
  LINUX_WRITE_BAD_FD,
  LINUX_WRITE_BUFFER_SEG_FAULT,
  LINUX_WRITE_INVALID_PEER_ADDRESS,
  LINUX_WRITE_QUOTA,
  LINUX_WRITE_LENGTH_TOO_BIG,
  LINUX_WRITE_INTERRUPT,
  LINUX_WRITE_INVALID,
  LINUX_WRITE_IO,
  LINUX_WRITE_NO_SPACE,
  LINUX_WRITE_PERMISSIONS,
  LINUX_WRITE_PIPE_CLOSED,
  LINUX_WRITE_UNKNOWN,
} LinuxWriteError;

typedef struct
{
  size_t count;
  LinuxWriteError error;
} LinuxWriteResult;

LinuxWriteResult linux_write(int fd, const unsigned char *data, size_t length)
{
  ssize_t count = syscall(SYS_write, fd, data, length);

  LinuxWriteError error = LINUX_WRITE_OK;
  if (count < 0)
  {
    switch (errno)
    {
    case EAGAIN:
      error = LINUX_WRITE_WOULD_BLOCK;
      break;

    case EBADF:
      error = LINUX_WRITE_BAD_FD;
      break;

    case EDESTADDRREQ:
      error = LINUX_WRITE_INVALID_PEER_ADDRESS;
      break;

    case EDQUOT:
      error = LINUX_WRITE_QUOTA;
      break;

    case EFAULT:
      error = LINUX_WRITE_BUFFER_SEG_FAULT;
      break;

    case EFBIG:
      error = LINUX_WRITE_LENGTH_TOO_BIG;
      break;

    case EINTR:
      error = LINUX_WRITE_INTERRUPT;
      break;

    case EINVAL:
      error = LINUX_WRITE_INVALID;
      break;

    case EIO:
      error = LINUX_WRITE_IO;
      break;

    case ENOSPC:
      error = LINUX_WRITE_NO_SPACE;
      break;

    case EPERM:
      error = LINUX_WRITE_PERMISSIONS;
      break;

    case EPIPE:
      error = LINUX_WRITE_PIPE_CLOSED;
      break;

    default:
      error = LINUX_WRITE_UNKNOWN;
      break;
    }
  }

  return (LinuxWriteResult){
      .count = count,
      .error = error,
  };
}

typedef enum
{
  LINUX_FDATASYNC_OK,
  LINUX_FDATASYNC_BAD_FD,
  LINUX_FDATASYNC_INTERRUPT,
  LINUX_FDATASYNC_IO,
  LINUX_FDATASYNC_NO_SPACE,
  LINUX_FDATASYNC_READ_ONLY_FILESYSTEM,
  LINUX_FDATASYNC_FD_NO_SYNC_SUPPORT,
  LINUX_FDATASYNC_QUOTA,
  LINUX_FDATASYNC_UNKNOWN,
} LinuxFDataSyncError;

LinuxFDataSyncError linux_fdatasync(int fd)
{
  if (syscall(SYS_fdatasync, fd) < 0)
  {
    switch (errno)
    {
    case EBADF:
      return LINUX_FDATASYNC_BAD_FD;

    case EINTR:
      return LINUX_FDATASYNC_INTERRUPT;

    case EIO:
      return LINUX_FDATASYNC_IO;

    case ENOSPC:
      return LINUX_FDATASYNC_NO_SPACE;

    case EROFS:
      return LINUX_FDATASYNC_READ_ONLY_FILESYSTEM;

    case EINVAL:
      return LINUX_FDATASYNC_FD_NO_SYNC_SUPPORT;

    case EDQUOT:
      return LINUX_FDATASYNC_QUOTA;

    default:
      return LINUX_FDATASYNC_UNKNOWN;
    }
  }

  return LINUX_FDATASYNC_OK;
}

typedef enum
{
  LINUX_UNLINK_OK,
  LINUX_UNLINK_ACCESS,
  LINUX_UNLINK_BUSY,
  LINUX_UNLINK_PATH_SEG_FAULT,
  LINUX_UNLINK_IO,
  LINUX_UNLINK_IS_DIRECTORY,
  LINUX_UNLINK_TOO_MANY_SYMBOLIC_LINKS,
  LINUX_UNLINK_NAME_TOO_LONG,
  LINUX_UNLINK_FILE_NOT_FOUND,
  LINUX_UNLINK_NO_MEMORY,
  LINUX_UNLINK_PERMISSIONS,
  LINUX_UNLINK_READ_ONLY_FILESYSTEM,
  LINUX_UNLINK_BAD_FD,
  LINUX_UNLINK_INVALID_FLAGS,
  LINUX_UNLINK_NOT_DIRECTORY,
  LINUX_UNLINK_UNKNOWN,
} LinuxUnlinkError;

LinuxUnlinkError linux_unlink(const char *path)
{
  LinuxUnlinkError error = LINUX_UNLINK_OK;
  if (syscall(SYS_unlink, path) < 0)
  {
    switch (errno)
    {
    case EACCES:
      return LINUX_UNLINK_ACCESS;

    case EBUSY:
      return LINUX_UNLINK_BUSY;

    case EFAULT:
      return LINUX_UNLINK_PATH_SEG_FAULT;

    case EIO:
      return LINUX_UNLINK_IO;

    case EISDIR:
      return LINUX_UNLINK_IS_DIRECTORY;

    case ELOOP:
      return LINUX_UNLINK_TOO_MANY_SYMBOLIC_LINKS;

    case ENAMETOOLONG:
      return LINUX_UNLINK_NAME_TOO_LONG;

    case ENOENT:
      return LINUX_UNLINK_FILE_NOT_FOUND;

    case ENOMEM:
      return LINUX_UNLINK_NO_MEMORY;

    case EPERM:
      return LINUX_UNLINK_PERMISSIONS;

    case EROFS:
      return LINUX_UNLINK_READ_ONLY_FILESYSTEM;

    case EBADF:
      return LINUX_UNLINK_BAD_FD;

    case EINVAL:
      return LINUX_UNLINK_INVALID_FLAGS;

    case ENOTDIR:
      return LINUX_UNLINK_NOT_DIRECTORY;

    default:
      return LINUX_UNLINK_UNKNOWN;
    }
  }

  return LINUX_UNLINK_OK;
}

typedef enum
{
  LINUX_CLOSE_OK,
  LINUX_CLOSE_BAD_FD,
  LINUX_CLOSE_INTERRUPT,
  LINUX_CLOSE_IO,
  LINUX_CLOSE_QUOTA,
  LINUX_CLOSE_UNKNOWN,
} LinuxCloseError;

LinuxCloseError linux_close(int fd)
{
  if (syscall(SYS_close, fd) < 0)
  {
    switch (errno)
    {
    case EBADF:
      return LINUX_CLOSE_BAD_FD;

    case EINTR:
      return LINUX_CLOSE_INTERRUPT;

    case EIO:
      return LINUX_CLOSE_IO;

    case ENOSPC:
    case EDQUOT:
      return LINUX_CLOSE_QUOTA;

    default:
      return LINUX_CLOSE_UNKNOWN;
    }
  }

  return LINUX_CLOSE_OK;
}

typedef enum
{
  LINUX_FTRUNCATE_OK,
  LINUX_FTRUNCATE_ACCESS,
  LINUX_FTRUNCATE_PATH_SEG_FAULT,
  LINUX_FTRUNCATE_INTERRUPT,
  LINUX_FTRUNCATE_LENGTH_INVALID,
  LINUX_FTRUNCATE_LENGTH_OR_FD_INVALID,
  LINUX_FTRUNCATE_IO,
  LINUX_FTRUNCATE_IS_DIRECTORY,
  LINUX_FTRUNCATE_TOO_MANY_SYMBOLIC_LINKS,
  LINUX_FTRUNCATE_PATH_TOO_LONG,
  LINUX_FTRUNCATE_FILE_NOT_FOUND,
  LINUX_FTRUNCATE_INVALID_PATH,
  LINUX_FTRUNCATE_PERMISSIONS,
  LINUX_FTRUNCATE_READ_ONLY_FILESYSTEM,
  LINUX_FTRUNCATE_BUSY,
  LINUX_FTRUNCATE_BAD_FD,
  LINUX_FTRUNCATE_UNKNOWN,
} LinuxFTruncateError;

LinuxFTruncateError linux_ftruncate(int fd, size_t size)
{
  if (syscall(SYS_ftruncate, fd, size) < 0)
  {
    switch (errno)
    {
    case EACCES:
      return LINUX_FTRUNCATE_ACCESS;

    case EFAULT:
      return LINUX_FTRUNCATE_PATH_SEG_FAULT;

    case EINTR:
      return LINUX_FTRUNCATE_INTERRUPT;

    case EFBIG:
      return LINUX_FTRUNCATE_LENGTH_INVALID;

    case EINVAL:
      return LINUX_FTRUNCATE_LENGTH_OR_FD_INVALID;

    case EIO:
      return LINUX_FTRUNCATE_IO;

    case EISDIR:
      return LINUX_FTRUNCATE_IS_DIRECTORY;

    case ELOOP:
      return LINUX_FTRUNCATE_TOO_MANY_SYMBOLIC_LINKS;

    case ENAMETOOLONG:
      return LINUX_FTRUNCATE_PATH_TOO_LONG;

    case ENOENT:
      return LINUX_FTRUNCATE_FILE_NOT_FOUND;

    case ENOTDIR:
      return LINUX_FTRUNCATE_INVALID_PATH;

    case EPERM:
      return LINUX_FTRUNCATE_PERMISSIONS;

    case EROFS:
      return LINUX_FTRUNCATE_READ_ONLY_FILESYSTEM;

    case ETXTBSY:
      return LINUX_FTRUNCATE_BUSY;

    case EBADF:
      return LINUX_FTRUNCATE_BAD_FD;

    default:
      return LINUX_FTRUNCATE_UNKNOWN;
    }
  }

  return LINUX_FTRUNCATE_OK;
}

typedef enum
{
  LINUX_FSTAT_OK,
  LINUX_FSTAT_ACCESS,
  LINUX_FSTAT_BAD_FD,
  LINUX_FSTAT_FAULT,
  LINUX_FSTAT_INVALID,
  LINUX_FSTAT_LOOP,
  LINUX_FSTAT_NAME_TOO_LONG,
  LINUX_FSTAT_NO_ENTRY,
  LINUX_FSTAT_NO_MEMORY,
  LINUX_FSTAT_NOT_DIRECTORY,
  LINUX_FSTAT_OVERFLOW,
  LINUX_FSTAT_UNKNOWN,
} LinuxFStatError;

typedef struct
{
  struct stat stat;
  LinuxFStatError error;
} LinuxFStatResult;

LinuxFStatResult linux_fstat(int fd)
{
  struct stat stat = {};
  LinuxFStatError error = LINUX_FSTAT_OK;
  if (syscall(SYS_fstat, fd, &stat) < 0)
  {
    switch (errno)
    {
    case EACCES:
      error = LINUX_FSTAT_ACCESS;
      break;

    case EBADF:
      error = LINUX_FSTAT_BAD_FD;
      break;

    case EFAULT:
      error = LINUX_FSTAT_FAULT;
      break;

    case EINVAL:
      error = LINUX_FSTAT_INVALID;
      break;

    case ELOOP:
      error = LINUX_FSTAT_LOOP;
      break;

    case ENAMETOOLONG:
      error = LINUX_FSTAT_NAME_TOO_LONG;
      break;

    case ENOENT:
      error = LINUX_FSTAT_NO_ENTRY;
      break;

    case ENOMEM:
      error = LINUX_FSTAT_NO_MEMORY;
      break;

    case ENOTDIR:
      error = LINUX_FSTAT_NOT_DIRECTORY;
      break;

    case EOVERFLOW:
      error = LINUX_FSTAT_OVERFLOW;
      break;

    default:
      error = LINUX_FSTAT_UNKNOWN;
      break;
    }
  }

  return (LinuxFStatResult){
      .stat = stat,
      .error = error,
  };
}

#endif
