import Foundation
import CoreGraphics
import ImageIO
import UniformTypeIdentifiers

enum ImageUtils {
    static func saveAsJPEG(image: CGImage, path: String, quality: CGFloat) throws {
        let fm = FileManager.default
        let outputURL = URL(fileURLWithPath: path)

        let directory = outputURL.deletingLastPathComponent()
        try fm.createDirectory(at: directory, withIntermediateDirectories: true)
        let tempURL = directory.appendingPathComponent(".\(outputURL.lastPathComponent).\(UUID().uuidString).tmp")

        guard let destination = CGImageDestinationCreateWithURL(
            tempURL as CFURL,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ) else {
            throw ImageUtilsError.cannotCreateDestination
        }

        let options: [CFString: Any] = [
            kCGImageDestinationLossyCompressionQuality: quality,
            // Normalize wide-gamut sources for consistent rendering on older clients.
            kCGImageDestinationOptimizeColorForSharing: true
        ]

        CGImageDestinationAddImage(destination, image, options as CFDictionary)

        guard CGImageDestinationFinalize(destination) else {
            try? fm.removeItem(at: tempURL)
            throw ImageUtilsError.cannotFinalizeImage
        }

        do {
            if fm.fileExists(atPath: outputURL.path) {
                _ = try fm.replaceItemAt(outputURL, withItemAt: tempURL)
            } else {
                try fm.moveItem(at: tempURL, to: outputURL)
            }
        } catch {
            try? fm.removeItem(at: tempURL)
            throw error
        }
    }
}

enum ImageUtilsError: Error {
    case cannotCreateDestination
    case cannotFinalizeImage
}
