class FedivertexException(Exception):
    pass


class DownloadError(FedivertexException):
    pass


class InteractionError(FedivertexException):
    pass
