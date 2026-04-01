import Foundation
import CoreGraphics
import ImageIO

package enum ImageProcessor {
    private static func createImageSource(sourcePath: String) throws -> CGImageSource {
        let url = URL(fileURLWithPath: sourcePath)
        let options = [kCGImageSourceShouldCache: false] as CFDictionary
        guard let imageSource = CGImageSourceCreateWithURL(url as CFURL, options) else {
            throw ImageProcessorError.cannotReadSource
        }
        return imageSource
    }

    private static func validateDimensions(
        imageSource: CGImageSource,
        sourcePath: String,
        maxPixels: Int,
        maxDimension: Int,
        maxCompressionRatio: Double
    ) throws -> (width: Int, height: Int) {
        guard let properties = CGImageSourceCopyPropertiesAtIndex(imageSource, 0, nil) as? [CFString: Any],
              let width = properties[kCGImagePropertyPixelWidth] as? Int,
              let height = properties[kCGImagePropertyPixelHeight] as? Int else {
            throw ImageProcessorError.cannotReadSource
        }

        guard width > 0, height > 0,
              width <= maxDimension, height <= maxDimension,
              width * height <= maxPixels else {
            throw ImageProcessorError.dimensionsTooLarge(width: width, height: height)
        }

        let fm = FileManager.default
        if let attrs = try? fm.attributesOfItem(atPath: sourcePath),
           let fileSize = attrs[.size] as? Int,
           fileSize > 0 {
            let uncompressedSize = width * height * 4
            let ratio = Double(uncompressedSize) / Double(fileSize)
            if ratio > maxCompressionRatio {
                throw ImageProcessorError.suspiciousCompressionRatio(ratio: ratio)
            }
        }

        return (width, height)
    }

    package static func validateDimensions(
        sourcePath: String,
        maxPixels: Int,
        maxDimension: Int,
        maxCompressionRatio: Double
    ) throws -> (width: Int, height: Int) {
        let imageSource = try createImageSource(sourcePath: sourcePath)
        return try validateDimensions(
            imageSource: imageSource,
            sourcePath: sourcePath,
            maxPixels: maxPixels,
            maxDimension: maxDimension,
            maxCompressionRatio: maxCompressionRatio
        )
    }

    private static func subsampleFactor(sourceMax: Int, targetSize: Int) -> Int {
        let ratio = sourceMax / targetSize
        if ratio >= 8 { return 8 }
        if ratio >= 4 { return 4 }
        if ratio >= 2 { return 2 }
        return 1
    }

    private static func generateDerivedJPEG(
        imageSource: CGImageSource,
        dims: (width: Int, height: Int),
        outputPath: String,
        size: Int,
        quality: Double,
        creationError: ImageProcessorError
    ) throws {
        var options: [CFString: Any] = [
            kCGImageSourceThumbnailMaxPixelSize: size,
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceShouldCacheImmediately: true
        ]
        let factor = subsampleFactor(sourceMax: max(dims.width, dims.height), targetSize: size)
        if factor > 1 {
            options[kCGImageSourceSubsampleFactor] = factor
        }

        guard let derived = CGImageSourceCreateThumbnailAtIndex(imageSource, 0, options as CFDictionary) else {
            throw creationError
        }

        try ImageUtils.saveAsJPEG(image: derived, path: outputPath, quality: quality)
    }

    package static func generateThumbnail(
        sourcePath: String,
        outputPath: String,
        size: Int,
        quality: Double,
        maxPixels: Int,
        maxDimension: Int,
        maxCompressionRatio: Double
    ) throws {
        try autoreleasepool {
            let imageSource = try createImageSource(sourcePath: sourcePath)
            let dims = try validateDimensions(
                imageSource: imageSource,
                sourcePath: sourcePath,
                maxPixels: maxPixels,
                maxDimension: maxDimension,
                maxCompressionRatio: maxCompressionRatio
            )
            try generateDerivedJPEG(
                imageSource: imageSource,
                dims: dims,
                outputPath: outputPath,
                size: size,
                quality: quality,
                creationError: .cannotCreateThumbnail
            )
        }
    }

    package static func generatePreview(
        sourcePath: String,
        outputPath: String,
        size: Int,
        quality: Double,
        maxPixels: Int,
        maxDimension: Int,
        maxCompressionRatio: Double
    ) throws {
        try autoreleasepool {
            let imageSource = try createImageSource(sourcePath: sourcePath)
            let dims = try validateDimensions(
                imageSource: imageSource,
                sourcePath: sourcePath,
                maxPixels: maxPixels,
                maxDimension: maxDimension,
                maxCompressionRatio: maxCompressionRatio
            )
            try generateDerivedJPEG(
                imageSource: imageSource,
                dims: dims,
                outputPath: outputPath,
                size: size,
                quality: quality,
                creationError: .cannotCreatePreview
            )
        }
    }

    package static func extractTimestamp(sourcePath: String) -> String? {
        autoreleasepool {
            guard let imageSource = try? createImageSource(sourcePath: sourcePath) else {
                return nil
            }

            guard let properties = CGImageSourceCopyPropertiesAtIndex(imageSource, 0, nil) as? [CFString: Any] else {
                return nil
            }

            if let exif = properties[kCGImagePropertyExifDictionary] as? [CFString: Any],
               let dateString = exif[kCGImagePropertyExifDateTimeOriginal] as? String {
                return dateString
            }

            if let tiff = properties[kCGImagePropertyTIFFDictionary] as? [CFString: Any],
               let dateString = tiff[kCGImagePropertyTIFFDateTime] as? String {
                return dateString
            }

            return nil
        }
    }

    package static func isImage(path: String) -> Bool {
        autoreleasepool {
            guard let imageSource = try? createImageSource(sourcePath: path) else {
                return false
            }
            return CGImageSourceGetType(imageSource) != nil
        }
    }
}

package enum ImageProcessorError: Error {
    case cannotReadSource
    case cannotCreateThumbnail
    case cannotCreatePreview
    case dimensionsTooLarge(width: Int, height: Int)
    case suspiciousCompressionRatio(ratio: Double)
}
