
class TransactionResult(dict):

    @property
    def transaction_id(self) -> str:
        return self.get("transaction_id", None)

    @property
    def unstaging_complete(self) -> bool:
        return self.get("unstaging_complete", None)

    def __str__(self):
        return f'TransactionResult:{super().__str__()}'
