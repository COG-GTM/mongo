/**
 *    Copyright (C) 2022-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#ifndef _WIN32

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/db/query/query_knobs_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/stdx/thread.h"
#include "mongo/transport/named_pipe/io_error_message.h"
#include "mongo/transport/named_pipe/named_pipe.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <fstream>  // IWYU pragma: keep
#include <string>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <fmt/format.h>
#include <sys/stat.h>

namespace mongo {

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

namespace {
// 'rc' must be the return code from POSIX-like OS I/O APIs.
inline bool hasSucceeded(int rc) {
    // POSIX-like I/O APIs return 0 for the successful operation.
    return rc == 0;
}

// Removes the named pipe and logs an error message when
// - either 'ignoreNoEntError' == true and there's an error other than the ENOENT error
// - or 'ignoreNoEntError' == false and there's any error
void removeNamedPipe(bool ignoreNoEntError, const char* pipeAbsolutePath) {
    if (!hasSucceeded(remove(pipeAbsolutePath))) {
        if (ignoreNoEntError && errno == ENOENT) {
            return;
        }

        LOGV2_ERROR(7097000,
                    "Failed to remove",
                    "error"_attr = getLastSystemErrorMessageFormatted("remove", pipeAbsolutePath));
    }
}
}  // namespace

NamedPipeOutput::NamedPipeOutput(const std::string& pipeDir,
                                 const std::string& pipeRelativePath,
                                 bool persistPipe)
    : _pipeAbsolutePath(pipeDir + pipeRelativePath), _ofs(), _persistPipe(persistPipe) {
    // Just in case that uncleaned-up named pipe is still there. This is a test-only implementation
    // and so, it should be fine to just remove it and ignore the ENOENT error.
    if (!persistPipe) {
        removeNamedPipe(true /*ignoreNoEntError*/, _pipeAbsolutePath.c_str());
        uassert(7005005,
                fmt::format("Failed to create a named pipe, error: {}",
                            getLastSystemErrorMessageFormatted("mkfifo", _pipeAbsolutePath)),
                hasSucceeded(mkfifo(_pipeAbsolutePath.c_str(), 0664)));
    }
}

NamedPipeOutput::~NamedPipeOutput() {
    close();
    // Makes sure that the named pipe is removed.
    if (!_persistPipe) {
        removeNamedPipe(false /*ignoreNoEntError*/, _pipeAbsolutePath.c_str());
    }
}

void NamedPipeOutput::open() {
    _ofs.open(_pipeAbsolutePath.c_str(), std::ios::binary | std::ios::app);
    if (!_ofs.is_open() || !_ofs.good()) {
        LOGV2_ERROR(7005009,
                    "Failed to open a named pipe",
                    "error"_attr = getLastSystemErrorMessageFormatted("open", _pipeAbsolutePath));
    }
}

int NamedPipeOutput::write(const char* data, int size) {
    uassert(7005011, "Output must have been opened before writing", _ofs.is_open());
    _ofs.write(data, size);
    if (_ofs.fail()) {
        uasserted(7239300,
                  fmt::format("Failed to write to a named pipe, error: {}",
                              getLastSystemErrorMessageFormatted("write", _pipeAbsolutePath)));
        return -1;
    }
    return size;
}

void NamedPipeOutput::close() {
    if (_ofs.is_open()) {
        _ofs.close();
    }
}

#undef MONGO_LOGV2_DEFAULT_COMPONENT
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace {
using Clock = stdx::chrono::steady_clock;

// How long a single wait for the pipe, a writer, or data may last before re-checking for
// interruption and the open deadline.
constexpr int kPollSliceMs = 100;

int remainingMs(Clock::time_point deadline) {
    auto ms = stdx::chrono::duration_cast<stdx::chrono::milliseconds>(deadline - Clock::now());
    return ms.count() > 0 ? static_cast<int>(ms.count()) : 0;
}
}  // namespace

NamedPipeInput::NamedPipeInput(const std::string& pipeRelativePath,
                               std::function<void()> checkForInterrupt)
    : _pipeAbsolutePath((externalPipeDir == "" ? kDefaultPipePath : externalPipeDir) +
                        pipeRelativePath),
      _checkForInterrupt(std::move(checkForInterrupt)) {
    uassert(7001100,
            fmt::format("Pipe path must not include '..' but {} does", _pipeAbsolutePath),
            _pipeAbsolutePath.find("..") == std::string::npos);
}

NamedPipeInput::~NamedPipeInput() {
    close();
}

void NamedPipeInput::doOpen() {
    const auto start = Clock::now();
    const auto deadline = start + stdx::chrono::milliseconds(kOpenTimeoutMs);

    // Retry the open every {1, 2, 4, 8, 16} ms (bounded by 'kOpenTimeoutMs' overall) in case the
    // pipe writer has not finished creating the pipe yet. O_NONBLOCK keeps open() from blocking
    // until a writer attaches and O_NOFOLLOW refuses a symlink in place of the pipe.
    int retries = 0;
    int sleepMs = 1;
    int fd;
    while ((fd = ::open(_pipeAbsolutePath.c_str(),
                        O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)) < 0) {
        uassert(ErrorCodes::FileNotOpen,
                fmt::format("error = {}",
                            getLastSystemErrorMessageFormatted("open", _pipeAbsolutePath)),
                errno == ENOENT || errno == EINTR);
        uassert(ErrorCodes::FileNotOpen,
                fmt::format("Named pipe {} was not created within {} ms",
                            _pipeAbsolutePath,
                            kOpenTimeoutMs),
                Clock::now() < deadline);
        if (_checkForInterrupt) {
            _checkForInterrupt();
        }
        stdx::this_thread::sleep_for(
            stdx::chrono::milliseconds(std::min(sleepMs, std::max(remainingMs(deadline), 1))));
        ++retries;
        if (retries % 1000 == 0 && sleepMs < 16) {
            sleepMs *= 2;
        }
    }
    _fd = fd;
    ScopeGuard closeOnError([&] { doClose(); });

    struct stat st;
    uassert(
        ErrorCodes::FileNotOpen,
        fmt::format("error = {}", getLastSystemErrorMessageFormatted("fstat", _pipeAbsolutePath)),
        ::fstat(_fd, &st) == 0);
    uassert(ErrorCodes::FileNotOpen,
            fmt::format("{} is not a named pipe", _pipeAbsolutePath),
            S_ISFIFO(st.st_mode));

    waitForWriter(deadline);
    _isGood = true;
    _isEof = false;
    closeOnError.dismiss();

    if (Clock::now() - start > stdx::chrono::seconds(1)) {
        LOGV2_WARNING(7184900,
                      "NamedPipeInput::doOpen() waited for pipe longer than 1 sec",
                      "_pipeAbsolutePath"_attr = _pipeAbsolutePath);
    }
}

void NamedPipeInput::waitForWriter(Clock::time_point deadline) {
    while (true) {
        // On a non-blocking FIFO, read() returns -1/EAGAIN when a writer is attached but has not
        // written yet, data when it has, and 0 when no writer is attached.
        char probe;
        auto n = ::read(_fd, &probe, 1);
        if (n == 1) {
            _pendingByte = probe;
            _hasPendingByte = true;
            return;
        }
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            uassert(ErrorCodes::FileNotOpen,
                    fmt::format("error = {}",
                                getLastSystemErrorMessageFormatted("read", _pipeAbsolutePath)),
                    errno == EINTR);
        }

        if (n == 0) {
#ifdef __linux__
            // Linux reports POLLHUP on a FIFO only once a writer that attached after our open() has
            // gone away, i.e. the writer closed without writing anything: an empty stream.
            pollfd pfd{_fd, POLLIN, 0};
            int rc = ::poll(&pfd, 1, std::min(kPollSliceMs, remainingMs(deadline)));
            if (rc > 0 && (pfd.revents & POLLHUP) && !(pfd.revents & POLLIN)) {
                return;
            }
#else
            stdx::this_thread::sleep_for(
                stdx::chrono::milliseconds(std::min(kPollSliceMs, remainingMs(deadline))));
#endif
        }

        uassert(ErrorCodes::FileNotOpen,
                fmt::format("No writer attached to named pipe {} within {} ms",
                            _pipeAbsolutePath,
                            kOpenTimeoutMs),
                Clock::now() < deadline);
        if (_checkForInterrupt) {
            _checkForInterrupt();
        }
    }
}

int NamedPipeInput::doRead(char* data, int size) {
    if (_hasPendingByte) {
        data[0] = _pendingByte;
        _hasPendingByte = false;
        return 1;
    }

    while (true) {
        auto n = ::read(_fd, data, size);
        if (n > 0) {
            return static_cast<int>(n);
        }
        if (n == 0) {
            _isEof = true;
            _isGood = false;
            return 0;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            _isGood = false;
            return 0;
        }

        // The writer is attached but has no data yet. Wait in bounded slices so that an
        // interrupted operation does not stay stuck on a silent writer.
        pollfd pfd{_fd, POLLIN, 0};
        ::poll(&pfd, 1, kPollSliceMs);
        if (_checkForInterrupt) {
            _checkForInterrupt();
        }
    }
}

void NamedPipeInput::doClose() {
    if (_fd >= 0) {
        ::close(_fd);
    }
    _fd = -1;
    _isGood = false;
    _isEof = false;
    _hasPendingByte = false;
}

bool NamedPipeInput::isOpen() const {
    return _fd >= 0;
}

bool NamedPipeInput::isGood() const {
    return _isGood;
}

bool NamedPipeInput::isFailed() const {
    return !_isGood;
}

bool NamedPipeInput::isEof() const {
    return _isEof;
}
}  // namespace mongo
#endif
