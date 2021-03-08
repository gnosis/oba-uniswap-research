from sgqlc.operation import Operation
from sgqlc.endpoint.http import HTTPEndpoint
from .uniswap_graphql_schema import uniswap_graphql_schema as schema

from functools import partial
from time import sleep
from urllib.error import URLError


class GraphQLError:
    pass

class UnrecoverableError(Exception):
    pass

class GraphQLClient:
    page_size = 500

    def __init__(self, url):
        self.endpoint = HTTPEndpoint(self.url)

    def paginated(self, query):
        """Abstracts the fact that results are paginated."""
        cur_page_size = self.page_size
        cur_skip = 0
        while cur_page_size == self.page_size:
            cur_page = query(skip=cur_skip, first=self.page_size)
            cur_page_size = len(cur_page)
            cur_skip += cur_page_size
            for i in cur_page:
                yield i

    # Apparently this is now the preferred way to do pagination
    def paginated_on_id(self, query):
        """Abstracts the fact that results are paginated."""
        cur_page_size = self.page_size
        last_id = None
        while cur_page_size == self.page_size:
            cur_page = query(first=self.page_size, last_id=last_id)
            cur_page_size = len(cur_page)
            for i in cur_page:
                yield i
            last_id = cur_page[-1].id


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
        token.symbol()
        token.decimals()

        data = self.endpoint(op)
        while True:
            data = self.endpoint(op)
            if 'errors' not in data.keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)

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
            try:
                data = self.endpoint(op)
            except URLError:
                data = {}
            if 'errors' not in data.keys() and \
               'data' in data.keys() and \
                'pair' in data['data'].keys() and \
                'reserve0' in data['data']['pair'].keys() and \
                'reserve1' in data['data']['pair'].keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)
        query = op + data
        return query.pair if hasattr(query, 'pair') else []

    def get_pair_reserves(self, id, **kwargs):
        op = Operation(schema.Query)
        pair = op.pair(
            id=id,
            **kwargs
        )
        pair.reserve0()
        pair.reserve1()

        while True:
            try:
                data = self.endpoint(op)
            except URLError:
                data = {}
            if 'errors' not in data.keys() and \
               'data' in data.keys() and \
                'pair' in data['data'].keys():
                if data['data']['pair'] is None:
                    raise UnrecoverableError()
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)
        query = op + data
        return query.pair if hasattr(query, 'pair') else []

    def get_token_day_price(self, token_id, date, **kwargs):
        op = Operation(schema.Query)

        token_day_data = op.token_day_datas(
            where={'date': date, 'token': token_id},
            **kwargs
        )
        token_day_data.price_usd()

        while True:
            try:
                data = self.endpoint(op)
            except URLError:
                data = {}
            if 'errors' not in data.keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)
        query = op + data
        if len(query.token_day_datas) != 1:
            raise UnrecoverableError()
        return query.token_day_datas[0].price_usd

    def get_token_block_price(self, token_id, **kwargs):
        op = Operation(schema.Query)

        token_data = op.token(
            id=token_id,
            **kwargs
        )
        token_data.derived_eth()

        while True:
            try:
                data = self.endpoint(op)
            except URLError:
                data = {}
            if 'errors' not in data.keys() and \
               'data' in data.keys() and \
                'token' in data['data'].keys():
                if data['data']['token'] is None:
                    raise UnrecoverableError()
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)
        query = op + data
        return query.token.derived_eth if hasattr(query, 'token') else []

    def get_pairs_page(self, pairs_filter, last_id, first, **kwargs):
        op = Operation(schema.Query)
        if last_id is not None:
            pairs_filter.update({"id_gt": last_id})
        pairs = op.pairs(
            where=pairs_filter,
            first=first,
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
            print(data['errors'])
            sleep(2)

        query = op + data
        return query.pairs if hasattr(query, 'pairs') else []

    def get_pairs(self, pairs_filter=dict(), **kwargs):
        """Get pairs."""
        return self.paginated_on_id(partial(self.get_pairs_page, pairs_filter, **kwargs))

    def get_pair_ids(self, token1, token2, **kwargs):
        """Get pairs and token ids."""
        op = Operation(schema.Query)
        filter = {
            'token0_in': [token1, token2],
            'token1_in': [token1, token2]
        }
        pairs = op.pairs(
            where=filter,
            first=1
        )
        pairs.id()
        pairs.token0().id()
        pairs.token1().id()

        while True:
            data = self.endpoint(op)
            if 'errors' not in data.keys():
                break
            print("Error getting data. Retrying in 2 secs.")
            sleep(2)

        query = op + data
        return query.pairs[0] if hasattr(query, 'pairs') else None

    def get_swaps_page(self, transactions_filter, skip, first):
        op = Operation(schema.Query)
        transactions = op.transactions(
            where=transactions_filter,
            skip=skip,
            first=first
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
