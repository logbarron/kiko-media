import Testing
import Foundation
import CoreGraphics
import ImageIO
@testable import KikoMediaCore

@Suite("Image Processing")
struct ImageProcessorTests {

    // MARK: - Thumbnail Generation

    @Test("Thumbnail of large image has max dimension <= 512px")
    func thumbnailDimensions() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let inputPath = tempDir.appendingPathComponent("input.jpg").path
        let outputPath = tempDir.appendingPathComponent("thumb.jpg").path

        // Create a 2000x1500 source image
        try TestImage.writeJPEG(to: inputPath, width: 2000, height: 1500)

        try ImageProcessor.generateThumbnail(sourcePath: inputPath, outputPath: outputPath, size: 512, quality: 0.85, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)

        let dims = try #require(TestImage.dimensions(at: outputPath))
        #expect(max(dims.width, dims.height) <= 512)
        #expect(min(dims.width, dims.height) > 0)
    }

    @Test("Thumbnail of landscape image preserves aspect ratio")
    func thumbnailAspectRatio() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let inputPath = tempDir.appendingPathComponent("input.jpg").path
        let outputPath = tempDir.appendingPathComponent("thumb.jpg").path

        try TestImage.writeJPEG(to: inputPath, width: 4000, height: 2000) // 2:1 ratio

        try ImageProcessor.generateThumbnail(sourcePath: inputPath, outputPath: outputPath, size: 512, quality: 0.85, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)

        let dims = try #require(TestImage.dimensions(at: outputPath))
        // Width should be ~512, height should be ~256 (2:1 ratio)
        #expect(dims.width == 512)
        #expect(dims.height >= 250 && dims.height <= 262)
    }

    @Test("Thumbnail of small image is not upscaled")
    func thumbnailNoUpscale() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let inputPath = tempDir.appendingPathComponent("input.jpg").path
        let outputPath = tempDir.appendingPathComponent("thumb.jpg").path

        try TestImage.writeJPEG(to: inputPath, width: 50, height: 50)

        try ImageProcessor.generateThumbnail(sourcePath: inputPath, outputPath: outputPath, size: 512, quality: 0.85, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)

        let dims = try #require(TestImage.dimensions(at: outputPath))
        // ImageIO's CreateThumbnailWithTransform may produce slightly different sizes
        // but should not upscale beyond original
        #expect(dims.width <= 50)
        #expect(dims.height <= 50)
    }

    // MARK: - Preview Generation

    @Test("Preview of large image has max dimension <= 1440px")
    func previewDimensions() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let inputPath = tempDir.appendingPathComponent("input.jpg").path
        let outputPath = tempDir.appendingPathComponent("preview.jpg").path

        // Use 2000x1500 (not 4000x3000) to avoid triggering decompression bomb detector
        // Solid-color synthetic images compress extremely well, pushing the ratio above 200x
        try TestImage.writeJPEG(to: inputPath, width: 2000, height: 1500)

        try ImageProcessor.generatePreview(sourcePath: inputPath, outputPath: outputPath, size: 1440, quality: 0.90, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)

        let dims = try #require(TestImage.dimensions(at: outputPath))
        #expect(max(dims.width, dims.height) <= 1440)
        #expect(min(dims.width, dims.height) > 0)
    }

    @Test("Thumbnail and preview normalize wide-gamut input for sharing")
    func outputsNormalizeWideGamut() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let inputPath = tempDir.appendingPathComponent("wide-gamut.jpg").path
        let thumbPath = tempDir.appendingPathComponent("thumb.jpg").path
        let previewPath = tempDir.appendingPathComponent("preview.jpg").path

        try writeDisplayP3JPEG(to: inputPath, width: 2400, height: 1600)

        #expect(try isDisplayP3Image(at: inputPath), "Test setup: source image must be tagged as Display P3")

        try ImageProcessor.generateThumbnail(sourcePath: inputPath, outputPath: thumbPath, size: 512, quality: 0.85, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)
        try ImageProcessor.generatePreview(sourcePath: inputPath, outputPath: previewPath, size: 1440, quality: 0.90, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)

        #expect(try !isDisplayP3Image(at: thumbPath), "Thumbnail should not remain Display P3")
        #expect(try !isDisplayP3Image(at: previewPath), "Preview should not remain Display P3")
    }

    // MARK: - Dimension Validation (Decompression Bomb Protection)

    @Test("validateDimensions accepts normal image")
    func validateNormal() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let path = tempDir.appendingPathComponent("normal.jpg").path
        try TestImage.writeJPEG(to: path, width: 1000, height: 800)

        let (w, h) = try ImageProcessor.validateDimensions(sourcePath: path, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)
        #expect(w == 1000)
        #expect(h == 800)
    }

    @Test("validateDimensions throws for suspicious compression ratio (decompression bomb)")
    func validateDecompressionBomb() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let path = tempDir.appendingPathComponent("bomb.jpg").path

        // Create a solid white image — compresses to near-zero bytes as JPEG,
        // producing a compression ratio well above the 200x threshold.
        let width = 2000
        let height = 1500
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        let bytesPerRow = width * 4
        let data = UnsafeMutablePointer<UInt8>.allocate(capacity: height * bytesPerRow)
        defer { data.deallocate() }

        // Fill with solid white (all 255)
        for i in 0..<(height * bytesPerRow) {
            data[i] = 255
        }

        let context = CGContext(
            data: data,
            width: width, height: height,
            bitsPerComponent: 8, bytesPerRow: bytesPerRow,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        )!
        let image = context.makeImage()!
        try ImageUtils.saveAsJPEG(image: image, path: path, quality: 0.85)

        do {
            _ = try ImageProcessor.validateDimensions(sourcePath: path, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)
            Issue.record("Expected suspiciousCompressionRatio for an over-compressed image")
        } catch let error as ImageProcessorError {
            switch error {
            case let .suspiciousCompressionRatio(ratio):
                #expect(ratio > 200.0)
            default:
                Issue.record("Expected suspiciousCompressionRatio, got \(error)")
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("validateDimensions throws for non-image file")
    func validateNonImage() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let path = tempDir.appendingPathComponent("notimage.txt").path
        try "hello world".write(toFile: path, atomically: true, encoding: .utf8)

        do {
            _ = try ImageProcessor.validateDimensions(sourcePath: path, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)
            Issue.record("Expected cannotReadSource for non-image input")
        } catch let error as ImageProcessorError {
            switch error {
            case .cannotReadSource:
                break
            default:
                Issue.record("Expected cannotReadSource, got \(error)")
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    // MARK: - Type Detection

    @Test("isImage returns true for JPEG")
    func isImageJPEG() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let path = tempDir.appendingPathComponent("test.jpg").path
        try TestImage.writeJPEG(to: path)

        #expect(ImageProcessor.isImage(path: path))
    }

    @Test("isImage returns false for text file")
    func isImageText() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let path = tempDir.appendingPathComponent("text.txt").path
        try "not an image".write(toFile: path, atomically: true, encoding: .utf8)

        #expect(!ImageProcessor.isImage(path: path))
    }

    @Test("isImage returns false for nonexistent file")
    func isImageMissing() {
        #expect(!ImageProcessor.isImage(path: "/tmp/does-not-exist-\(UUID().uuidString).jpg"))
    }

    // MARK: - Timestamp Extraction

    @Test("extractTimestamp returns nil for image without EXIF")
    func timestampNoExif() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        // Synthetic test images have no EXIF data
        let path = tempDir.appendingPathComponent("noexif.jpg").path
        try TestImage.writeJPEG(to: path)

        let timestamp = ImageProcessor.extractTimestamp(sourcePath: path)
        #expect(timestamp == nil)
    }

    @Test("extractTimestamp returns nil for nonexistent file")
    func timestampMissing() {
        let result = ImageProcessor.extractTimestamp(sourcePath: "/tmp/does-not-exist-\(UUID().uuidString).jpg")
        #expect(result == nil)
    }

    // MARK: - EXIF Stripping

    @Test("Generated thumbnail strips EXIF metadata from source image")
    func thumbnailStripsEXIF() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let inputPath = tempDir.appendingPathComponent("with-exif.jpg").path
        let outputPath = tempDir.appendingPathComponent("thumb.jpg").path

        // Create a JPEG that has EXIF metadata embedded
        try TestImage.writeJPEGWithEXIF(to: inputPath, width: 500, height: 500)

        // Verify source actually has EXIF
        let sourceURL = URL(fileURLWithPath: inputPath) as CFURL
        let sourceImage = CGImageSourceCreateWithURL(sourceURL, nil)!
        let sourceProps = CGImageSourceCopyPropertiesAtIndex(sourceImage, 0, nil) as? [CFString: Any]
        let sourceExif = sourceProps?[kCGImagePropertyExifDictionary]
        #expect(sourceExif != nil, "Test setup: source image must have EXIF")

        // Generate thumbnail
        try ImageProcessor.generateThumbnail(sourcePath: inputPath, outputPath: outputPath, size: 512, quality: 0.85, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)

        // Verify thumbnail has no EXIF metadata
        let outputURL = URL(fileURLWithPath: outputPath) as CFURL
        let outputImage = CGImageSourceCreateWithURL(outputURL, nil)!
        let outputProps = CGImageSourceCopyPropertiesAtIndex(outputImage, 0, nil) as? [CFString: Any]
        let outputExif = outputProps?[kCGImagePropertyExifDictionary] as? [CFString: Any]
        // These assertions pass whether outputExif is nil (no EXIF dict at all) or the key is absent
        #expect(outputExif?[kCGImagePropertyExifDateTimeOriginal] == nil, "Thumbnail should not leak EXIF DateTimeOriginal")
        #expect(outputExif?[kCGImagePropertyExifLensMake] == nil, "Thumbnail should not leak EXIF LensMake")
    }

    @Test("validateDimensions throws for image exceeding maxDimension")
    func validateMaxDimensionExceeded() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let path = tempDir.appendingPathComponent("large.jpg").path
        try TestImage.writeJPEG(to: path, width: 100, height: 100)

        // Use a very low maxDimension to trigger the guard
        do {
            _ = try ImageProcessor.validateDimensions(
                sourcePath: path,
                maxPixels: 250_000_000,
                maxDimension: 50,  // 100x100 image exceeds this
                maxCompressionRatio: 200.0
            )
            Issue.record("Expected dimensionsTooLarge when maxDimension is below the source dimensions")
        } catch let error as ImageProcessorError {
            switch error {
            case let .dimensionsTooLarge(width, height):
                #expect(width == 100)
                #expect(height == 100)
            default:
                Issue.record("Expected dimensionsTooLarge, got \(error)")
            }
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Generated preview strips EXIF metadata from source image")
    func previewStripsEXIF() throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kiko-imgtest-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let inputPath = tempDir.appendingPathComponent("with-exif.jpg").path
        let outputPath = tempDir.appendingPathComponent("preview.jpg").path

        try TestImage.writeJPEGWithEXIF(to: inputPath, width: 2000, height: 1500)

        try ImageProcessor.generatePreview(sourcePath: inputPath, outputPath: outputPath, size: 1440, quality: 0.90, maxPixels: 250_000_000, maxDimension: 20_000, maxCompressionRatio: 200.0)

        let outputURL = URL(fileURLWithPath: outputPath) as CFURL
        let outputImage = CGImageSourceCreateWithURL(outputURL, nil)!
        let outputProps = CGImageSourceCopyPropertiesAtIndex(outputImage, 0, nil) as? [CFString: Any]
        let outputExif = outputProps?[kCGImagePropertyExifDictionary] as? [CFString: Any]
        #expect(outputExif?[kCGImagePropertyExifDateTimeOriginal] == nil, "Preview should not leak EXIF DateTimeOriginal")
        #expect(outputExif?[kCGImagePropertyExifLensMake] == nil, "Preview should not leak EXIF LensMake")
    }

}

private enum ColorSpaceTestError: Error {
    case unsupportedDisplayP3
    case cannotCreateImage
    case cannotCreateDestination
    case cannotFinalizeDestination
    case cannotReadImage
}

private func writeDisplayP3JPEG(to path: String, width: Int, height: Int, quality: CGFloat = 0.90) throws {
    guard let displayP3 = CGColorSpace(name: CGColorSpace.displayP3) else {
        throw ColorSpaceTestError.unsupportedDisplayP3
    }

    let source = TestImage.make(width: width, height: height)
    guard let image = source.copy(colorSpace: displayP3) else {
        throw ColorSpaceTestError.cannotCreateImage
    }

    let url = URL(fileURLWithPath: path) as CFURL
    guard let destination = CGImageDestinationCreateWithURL(url, "public.jpeg" as CFString, 1, nil) else {
        throw ColorSpaceTestError.cannotCreateDestination
    }

    let options: [CFString: Any] = [
        kCGImageDestinationLossyCompressionQuality: quality
    ]
    CGImageDestinationAddImage(destination, image, options as CFDictionary)

    guard CGImageDestinationFinalize(destination) else {
        throw ColorSpaceTestError.cannotFinalizeDestination
    }
}

private func decodedColorSpaceName(at path: String) throws -> String? {
    let url = URL(fileURLWithPath: path) as CFURL
    guard let source = CGImageSourceCreateWithURL(url, nil),
          let image = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
        throw ColorSpaceTestError.cannotReadImage
    }
    return image.colorSpace?.name as String?
}

private func isDisplayP3Image(at path: String) throws -> Bool {
    if try decodedColorSpaceName(at: path) == (CGColorSpace.displayP3 as String) {
        return true
    }

    let url = URL(fileURLWithPath: path) as CFURL
    guard let source = CGImageSourceCreateWithURL(url, nil),
          let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any] else {
        throw ColorSpaceTestError.cannotReadImage
    }

    if let profileName = properties[kCGImagePropertyProfileName] as? String {
        return profileName.localizedCaseInsensitiveContains("display p3")
    }
    return false
}
