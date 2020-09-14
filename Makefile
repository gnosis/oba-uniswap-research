update-graphql-schema:
	python -m sgqlc.introspection --exclude-deprecated --exclude-description https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2 src/uniswap_graphql_schema.json
	sgqlc-codegen src/uniswap_graphql_schema.json src/uniswap_graphql_schema.py
	rm src/uniswap_graphql_schema.json

