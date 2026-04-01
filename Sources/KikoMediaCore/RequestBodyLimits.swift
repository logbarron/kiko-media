package enum RequestBodyLimits {
    private static let jsonSpec: (fallback: Int, range: ClosedRange<Int>) = {
        guard let spec = Config.intDefaults["JSON_MAX_BODY_BYTES"],
              let range = spec.range else {
            preconditionFailure("Missing JSON_MAX_BODY_BYTES spec")
        }
        return (spec.fallback, range)
    }()

    // Explicit cap for all JSON request bodies decoded by kiko-media routes.
    // Keep this far below upload file size limits enforced by tusd.
    package static let defaultJSONMaxBytes = jsonSpec.fallback
    package static let minJSONMaxBytes = jsonSpec.range.lowerBound
    package static let maxJSONMaxBytes = jsonSpec.range.upperBound
}
