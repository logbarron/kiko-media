import Foundation

package enum VolumeUtils {
    package static func isMounted(volumeContainingPath path: String) -> Bool {
        let pathComponents = path.split(separator: "/")
        guard pathComponents.count >= 2,
              pathComponents[0] == "Volumes" else {
            return false
        }
        let volumePath = "/Volumes/\(pathComponents[1])"

        let volumeURL = URL(fileURLWithPath: volumePath)
        do {
            let values = try volumeURL.resourceValues(forKeys: [.isVolumeKey])
            return values.isVolume == true
        } catch {
            return false
        }
    }
}
