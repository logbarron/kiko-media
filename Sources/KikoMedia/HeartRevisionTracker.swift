actor HeartRevisionTracker {
    private var revision: Int

    init(initialValue: Int = 0) {
        self.revision = max(0, initialValue)
    }

    func current() -> Int {
        revision
    }

    @discardableResult
    func bump() -> Int {
        if revision < Int.max {
            revision += 1
        }
        return revision
    }
}
