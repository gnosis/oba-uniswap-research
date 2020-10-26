from sgqlc.operation import Operation
from sgqlc.endpoint.http import HTTPEndpoint
from .uniswap_graphql_schema import uniswap_graphql_schema as schema

from functools import partial
from time import sleep


class GraphQLError:
    pass

class GraphQLClient:
    page_size = 1000

    def __init__(self, url):
        self.endpoint = HTTPEndpoint(self.url)

    def paginated(self, query):
        """Abstracts the fact that results are paginated."""
        cur_page_size = self.page_size
        cur_skip = 0
        while cur_page_size == self.page_size:
            cur_page = query(skip=cur_skip)
            cur_page_size = len(cur_page)
            cur_skip += cur_page_size
            for i in cur_page:
                yield i
            sleep(1)


class UniswapClient(GraphQLClient):
    url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

    def __init__(self):
        super().__init__(self.url)

    def get_token(self, id, **kwargs):
        op = Operation(schema.Query)
        token = op.token(
            id=id,
            **kwargs
        )
        token.id()
        token.symbol()
        token.decimals()

        data = self.endpoint(op)
        if 'errors' in data.keys():
            raise GraphQLError()

        query = op + data
        return query.token if hasattr(query, 'token') else None

    def get_pair(self, id, **kwargs):
        op = Operation(schema.Query)
        pair = op.pair(
            id=id,
            **kwargs
        )
        pair.id()
        pair.token0().symbol()
        pair.token0().id()
        pair.token0().decimals()
        pair.token0_price()
        pair.volume_token0()
        pair.reserve0()
        pair.token1().symbol()
        pair.token1().id()
        pair.token1().decimals()
        pair.token1_price()
        pair.volume_token1()
        pair.reserve1()
        pair.volume_usd()
        pair.reserve_eth()
        pair.reserve_usd()

        while True:
            data = self.endpoint(op)
            if 'errors' not in data.keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)

        query = op + data
        return query.pair if hasattr(query, 'pair') else []

    def get_pairs_page(self, pairs_filter, skip, **kwargs):
        op = Operation(schema.Query)
        pairs = op.pairs(
            where=pairs_filter,
            skip=skip,
            first=self.page_size,
            **kwargs
        )
        pairs.id()
        pairs.token0().symbol()
        pairs.token0().id()
        pairs.token0().decimals()
        pairs.token0_price()
        pairs.volume_token0()
        pairs.reserve0()
        pairs.token1().symbol()
        pairs.token1().id()
        pairs.token1().decimals()
        pairs.token1_price()
        pairs.volume_token1()
        pairs.reserve1()
        pairs.volume_usd()
        pairs.reserve_eth()
        pairs.reserve_usd()

        while True:
            data = self.endpoint(op)
            if 'errors' not in data.keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)

        query = op + data
        return query.pairs if hasattr(query, 'pairs') else []

    def get_pairs(self, pairs_filter=dict(), **kwargs):
        """Get pairs."""
        return self.paginated(partial(self.get_pairs_page, pairs_filter, **kwargs))

    def get_swaps_page(self, transactions_filter, skip):
        op = Operation(schema.Query)
        transactions = op.transactions(
            where=transactions_filter,
            skip=skip,
            first=self.page_size
        )
        transactions.block_number()
        transactions.swaps().log_index()
        transactions.swaps().pair().token0().symbol()
        transactions.swaps().pair().token1().symbol()
        transactions.swaps().amount0_in()
        transactions.swaps().amount1_in()
        transactions.swaps().amount0_out()
        transactions.swaps().amount1_out()
        transactions.swaps().amount_usd()

        while True:
            data = self.endpoint(op)
            if 'errors' not in data.keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)

        query = op + data

        if hasattr(query, 'transactions'):
            return query.transactions
        return []

    def get_swaps(self, transactions_filter=dict()):
        """Get swap transactions."""
        return self.paginated(partial(self.get_swaps_page, transactions_filter))

    def get_mints_page(self, transactions_filter, skip):
        op = Operation(schema.Query)
        transactions = op.transactions(
            where=transactions_filter,
            skip=skip,
            first=self.page_size
        )
        transactions.block_number()
        transactions.mints().log_index()
        transactions.mints().pair().token0().symbol()
        transactions.mints().pair().token1().symbol()
        transactions.mints().amount0()
        transactions.mints().amount1()
        transactions.mints().amount_usd()
        transactions.mints().liquidity()

        while True:
            data = self.endpoint(op)
            if 'errors' not in data.keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)

        query = op + data

        if hasattr(query, 'transactions'):
            return query.transactions
        return []

    def get_mints(self, transactions_filter=dict()):
        """Get mint transactions (add liquidity)."""
        return self.paginated(partial(self.get_mints_page, transactions_filter))
