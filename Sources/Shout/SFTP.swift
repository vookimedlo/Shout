//
//  SFTP.swift
//  Shout
//
//  Created by Vladislav Alexeev on 6/20/18.
//

import Foundation
import CSSH

/// Manages an SFTP session
public class SFTP {
    
    /// Direct bindings to libssh2_sftp
    private class SFTPHandle {
        
        // Recommended buffer size accordingly to the docs:
        // https://www.libssh2.org/libssh2_sftp_write.html
        fileprivate static let bufferSize = 32768
        
        private let cSession: OpaquePointer
        private let sftpHandle: OpaquePointer
        private var buffer = [Int8](repeating: 0, count: SFTPHandle.bufferSize)
        
        init(cSession: OpaquePointer, sftpSession: OpaquePointer, remotePath: String, flags: Int32, mode: Int32, openType: Int32 = LIBSSH2_SFTP_OPENFILE) throws {
            guard let sftpHandle = libssh2_sftp_open_ex(
                sftpSession,
                remotePath,
                UInt32(remotePath.count),
                UInt(flags),
                Int(mode),
                openType) else {
                    throw SSHError.mostRecentError(session: cSession, backupMessage: "libssh2_sftp_open_ex failed")
            }
            self.cSession = cSession
            self.sftpHandle = sftpHandle
        }

        func read() -> ReadWriteProcessor.ReadResult {
            let result = libssh2_sftp_read(sftpHandle, &buffer, SFTPHandle.bufferSize)
            return ReadWriteProcessor.processRead(result: result, buffer: &buffer, session: cSession)
        }
        
        func write(_ data: Data) -> ReadWriteProcessor.WriteResult {
            let result = data.withUnsafeBytes { (pointer: UnsafePointer<Int8>) -> Int in
                return libssh2_sftp_write(sftpHandle, pointer, data.count)
            }
            return ReadWriteProcessor.processWrite(result: result, session: cSession)
        }
        
        func readDir(_ attrs: inout LIBSSH2_SFTP_ATTRIBUTES) -> ReadWriteProcessor.ReadResult {
            let result = libssh2_sftp_readdir_ex(sftpHandle, &buffer, SFTPHandle.bufferSize, nil, 0, &attrs)
            return ReadWriteProcessor.processRead(result: Int(result), buffer: &buffer, session: cSession)
        }
        
        deinit {
            libssh2_sftp_close_handle(sftpHandle)
        }
        
    }
    
    private let cSession: OpaquePointer
    private let sftpSession: OpaquePointer
        
    init(cSession: OpaquePointer) throws {
        guard let sftpSession = libssh2_sftp_init(cSession) else {
            throw SSHError.mostRecentError(session: cSession, backupMessage: "libssh2_sftp_init failed")
        }
        self.cSession = cSession
        self.sftpSession = sftpSession
    }
    
    private func link(_ remotePath: Data, target: inout Data, linkType: Int32) -> (ReadWriteProcessor.WriteResult) {
        var buffer = [Int8](repeating: 0, count: 1024)
        let result = remotePath.withUnsafeBytes { (bytes) -> Int in
            let pointer = bytes.baseAddress!.assumingMemoryBound(to: CChar.self)
            return Int(libssh2_sftp_symlink_ex(sftpSession,
                                               pointer,
                                               UInt32(remotePath.count),
                                               &buffer,
                                               UInt32(buffer.count),
                                               linkType))
                //LIBSSH2_SFTP_REALPATH
                //LIBSSH2_SFTP_READLINK
                //LIBSSH2_SFTP_SYMLINK
        }
        
        let bufferUint8 = buffer.map { (signed) -> UInt8 in
            return UInt8(signed)
        }
        
        target.removeAll()
        target.append(contentsOf: bufferUint8)
        return ReadWriteProcessor.processWrite(result: result, session: cSession)
    }
    
    private func mkdir(_ remotePath: Data, permissions: FilePermissions) -> ReadWriteProcessor.WriteResult {
        let result = remotePath.withUnsafeBytes { (bytes) -> Int in
            let pointer = bytes.baseAddress!.assumingMemoryBound(to: CChar.self)
            return Int(libssh2_sftp_mkdir_ex(sftpSession,
                                             pointer,
                                             UInt32(remotePath.count),
                                             Int(permissions.rawValue)))
        }
        
        return ReadWriteProcessor.processWrite(result: result, session: cSession)
    }
    
    private func stat(_ remotePath: Data, attrs: inout LIBSSH2_SFTP_ATTRIBUTES, statType: Int32) -> ReadWriteProcessor.WriteResult {
        let result = remotePath.withUnsafeBytes { (bytes) -> Int in
            let pointer = bytes.baseAddress!.assumingMemoryBound(to: CChar.self)
            return Int(libssh2_sftp_stat_ex(sftpSession,
                                            pointer,
                                            UInt32(remotePath.count),
                                            statType,
                                            &attrs))
            // LIBSSH2_SFTP_STAT
            // LIBSSH2_SFTP_LSTAT
            // LIBSSH2_SFTP_SETSTAT
        }

        return ReadWriteProcessor.processWrite(result: result, session: cSession)
    }

    /// Makes a new directory on the remote server
    /// - Parameter remotePath: the path to the new directory on the remote server
    /// - Parameter permissions: the file permissions to create the new directory with; defaults to FilePermissions.directoryDefault
    public func mkdir(remotePath: String, permissions: FilePermissions = .directoryDefault) throws {
        guard let data = remotePath.data(using: .utf8) else {
            throw SSHError.genericError("Unable to convert string to utf8 data")
        }

        var wasSent = false
        while !wasSent {
            switch mkdir(data, permissions: permissions) {
            case .written(_):
                wasSent = true
            case .eagain:
                break
            case .error(let error):
                throw error
            }
        }
    }

    /// Lists a directory content from the remote server
    /// - Parameter remotePath: the path to the existing directory on the remote server to list
    public func ls(remotePath: String) throws -> [String:LIBSSH2_SFTP_ATTRIBUTES]  {
        let sftpHandle = try SFTPHandle(
                cSession: cSession,
                sftpSession: sftpSession,
                remotePath: remotePath,
                flags: LIBSSH2_FXF_READ,
                mode: 0,
                openType: LIBSSH2_SFTP_OPENDIR
        )

        var files = [String:LIBSSH2_SFTP_ATTRIBUTES]()
        var attrs = LIBSSH2_SFTP_ATTRIBUTES()

        var dataLeft = true
        while dataLeft {
            switch sftpHandle.readDir(&attrs) {
            case .data(let data):
                guard let name = String(data: data, encoding: .utf8) else {
                    throw SSHError.genericError("unable to convert data to utf8 string")
                }
                files[name] = attrs
            case .done:
                dataLeft = false
            case .eagain:
                break
            case .error(let error):
                throw error
            }
        }
        return files
    }
    
    public func realpath(remotePath: String) throws -> String {
        guard let data = remotePath.data(using: .utf8) else {
            throw SSHError.genericError("Unable to convert string to utf8 data")
        }
        
        var targetData = Data()
        var wasSent = false
        while !wasSent {
            switch link(data, target: &targetData, linkType: LIBSSH2_SFTP_REALPATH) {
            case .written(_):
                wasSent = true
            case .eagain:
                break
            case .error(let error):
                throw error
            }
        }
        
        let target = try targetData.withUnsafeBytes { (bytes) -> String in
            let pointer = bytes.baseAddress!.assumingMemoryBound(to: CChar.self)
            guard let target = String(cString: pointer, encoding: .utf8) else {
                throw SSHError.genericError("unable to convert data to utf8 string")
            }
            return target
        }

        return target
    }
    
    public func stat(remotePath: String) throws -> LIBSSH2_SFTP_ATTRIBUTES {
        guard let data = remotePath.data(using: .utf8) else {
            throw SSHError.genericError("Unable to convert string to utf8 data")
        }

        var attrs = LIBSSH2_SFTP_ATTRIBUTES()
        var wasSent = false
        while !wasSent {
            switch stat(data, attrs: &attrs, statType: LIBSSH2_SFTP_STAT) {
            case .written(_):
                wasSent = true
            case .eagain:
                break
            case .error(let error):
                throw error
            }
        }

        return attrs
    }

    /// Download a file from the remote server to the local device
    ///
    /// - Parameters:
    ///   - remotePath: the path to the existing file on the remote server to download
    ///   - localURL: the location on the local device whether the file should be downloaded to
    /// - Throws: SSHError if file can't be created or download fails
    public func download(remotePath: String, localURL: URL) throws {
        let sftpHandle = try SFTPHandle(
            cSession: cSession,
            sftpSession: sftpSession,
            remotePath: remotePath,
            flags: LIBSSH2_FXF_READ,
            mode: 0
        )
        
        guard FileManager.default.createFile(atPath: localURL.path, contents: nil, attributes: nil),
            let fileHandle = try? FileHandle(forWritingTo: localURL) else {
            throw SSHError.genericError("couldn't create file at \(localURL.path)")
        }
        
        defer { fileHandle.closeFile() }

        var dataLeft = true
        while dataLeft {
            switch sftpHandle.read() {
            case .data(let data):
                fileHandle.write(data)
            case .done:
                dataLeft = false
            case .eagain:
                break
            case .error(let error):
                throw error
            }
        }
    }
    
    /// Upload a file from the local device to the remote server
    ///
    /// - Parameters:
    ///   - localURL: the path to the existing file on the local device
    ///   - remotePath: the location on the remote server whether the file should be uploaded to
    ///   - permissions: the file permissions to create the new file with; defaults to FilePermissions.default
    /// - Throws: SSHError if local file can't be read or upload fails
    public func upload(localURL: URL, remotePath: String, permissions: FilePermissions = .default) throws {
        let data = try Data(contentsOf: localURL, options: .alwaysMapped)
        try upload(data: data, remotePath: remotePath, permissions: permissions)
    }
    
    /// Upload data to a file on the remote server
    ///
    /// - Parameters:
    ///   - string: String to be uploaded as a file
    ///   - remotePath: the location on the remote server whether the file should be uploaded to
    ///   - permissions: the file permissions to create the new file with; defaults to FilePermissions.default
    /// - Throws: SSHError if string is not valid or upload fails
    public func upload(string: String, remotePath: String, permissions: FilePermissions = .default) throws {
        guard let data = string.data(using: .utf8) else {
            throw SSHError.genericError("Unable to convert string to utf8 data")
        }
        try upload(data: data, remotePath: remotePath, permissions: permissions)
    }
    
    /// Upload data to a file on the remote server
    ///
    /// - Parameters:
    ///   - data: Data to be uploaded as a file
    ///   - remotePath: the location on the remote server whether the file should be uploaded to
    ///   - permissions: the file permissions to create the new file with; defaults to FilePermissions.default
    /// - Throws: SSHError if upload fails
    public func upload(data: Data, remotePath: String, permissions: FilePermissions = .default) throws {
        let sftpHandle = try SFTPHandle(
            cSession: cSession,
            sftpSession: sftpSession,
            remotePath: remotePath,
            flags: LIBSSH2_FXF_WRITE | LIBSSH2_FXF_CREAT,
            mode: LIBSSH2_SFTP_S_IFREG | permissions.rawValue
        )
        
        var offset = 0
        while offset < data.count {
            let upTo = Swift.min(offset + SFTPHandle.bufferSize, data.count)
            let subdata = data.subdata(in: offset ..< upTo)
            if subdata.count > 0 {
                switch sftpHandle.write(subdata) {
                case .written(let bytesSent):
                    offset += bytesSent
                case .eagain:
                    break
                case .error(let error):
                    throw error
                }
            }
        }
    }
    
    deinit {
        libssh2_sftp_shutdown(sftpSession)
    }
    
}
