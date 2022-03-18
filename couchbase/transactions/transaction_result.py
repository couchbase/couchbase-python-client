
class TransactionResult(dict):

    @property
    def transaction_id(self) -> str:
        self.get("transaction_id")

    @property
    def unstaging_complete(self) -> bool:
        self.get("unstaging_complete")

    def __str__(self):
        return f'TransactionResult{{{str(self)}}}'
