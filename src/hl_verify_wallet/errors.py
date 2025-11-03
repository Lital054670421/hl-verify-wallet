class HLRequestError(RuntimeError):
    pass

class HLRateLimitError(RuntimeError):
    pass

class HLTooManyFillsError(RuntimeError):
    """Returned fills reached or exceeded the 10k cap for userFillsByTime."""
    pass

class HLNoResultsError(RuntimeError):
    """No results returned repeatedly (possibly wrong wallet address)."""
    pass
