# Asset Pricing
A package to manage asset pricing with the help of Apache Spark. It supports Foreign Exchange (FX) spots initially. It can be extended to support other Asset classes besides FX (like for example Corporate Actions, Securities, Alternative Investments, Cash and Cash Equivalents, Derivatives, Fixed Income -beyond corporate bonds, Insurance Products, Cryptocurrency/Digital Assets) and their corresponding instrument types/products (like for FX we could support FX Spot, FX Futures, FX Forwards, FX Options, and FX Swaps).

Useful to perform a number of operations like portfolio valuations and investment reporting.

It uses poetry (see pyproject.toml) for dependency management and packaging, flake8 (see .flake8) as linter to ensure PEP 8 styling is respected, mypy to check for static typing (see mypy.ini), pydantic (see fx/config.py) for data validation.

Asset Pricing is a modular project to ensure that development teams can handle pricing for different assets without interfering with each other to speed up development or simply to ensure complete abstraction. This means that for each asset type (one or more investment vehicle) a separate directory will exists under the main module (asset-pricing).)

Since datafeeds for development purposes can be expensive we focus on just two profiles (a profile is a definition that determines how pricing is acquired for each instrument). A first profile is a simple example for just one datafeed for each instrument type like it is the case of "alphavantage_fx_batch". A second profile called "mocked_fx_spot_batch" mocks feeds to ensure we can test with any amount of data. This means that  we can easily add support for different feeds while keeping the changes at a minimum. Mocking is supposed to happen based on data seeds (see seed_data.json) that offer the starting point to generate just about any number of prices for the quoted instrument.

There are two main steps, the "fetch" step which retrieved and stores raw data somewhere and the "process" step which parses the raw data and populates the database. These steps are both "profile" dependent.

```
nu@Nestors-MacBook-Pro asset-pricing % tree -a --gitignore -I .git
.
├── .flake8
├── .gitignore
├── README.md
├── asset_pricing
│   ├── __init__.py
│   └── fx
│       ├── __init__.py
│       └── spot
│           ├── __init__.py
│           ├── config.py
│           ├── config.py.1
│           ├── config.py.2
│           ├── config.py.3
│           ├── fetch.py
│           ├── fetch.py.1
│           ├── fetch.py.2
│           ├── fetch.py.3
│           ├── models.py
│           ├── process.py
│           ├── process.py.1
│           ├── process.py.2
│           ├── process.py.3
│           ├── seed_data.json
│           └── sql
│               └── fx_spot.sql
├── mypy.ini
├── poetry.lock
└── pyproject.toml
```

## Preconfitions
- A .config.json file includes all the necessary configuration information and it is mandatory as argument for all operations. All asset_pricing tables use precisely that schema. This setup ensures that code remains consistent across different environments, allowing developers to test locally and DevOps to deploy in scalable cloud environments by simply modifying the .config.json file.
```
{
  "spark": {
    "app_name": "AssetPricingFXSpot",
    "master": "local[*]",
    "config": {
      "spark.executor.memory": "2g",
      "spark.executor.cores": "2",
      "spark.driver.memory": "1g",
      "spark.jars": "/Users/nu/Downloads/postgresql-42.7.4.jar, /Users/nu/Downloads/hadoop-azure-3.3.1.jar, /Users/nu/Downloads/azure-storage-8.6.6.jar, /Users/nu/Downloads/jetty-util-9.3.24.v20180605.jar, /Users/nu/Downloads/jetty-util-ajax-9.3.24.v20180605.jar, /Users/nu/Downloads/hadoop-common-3.3.1.jar"
    }
  },
  "asset_pricing_batch": {
    "fx": {
      "spot": {
        "profiles": [
          { 
            "profile_name": "mocked_fx_spot_batch",
            "api_type": "mocked_fx_spot",
            "tickers": ["EUR/USD", "USD/JPY", "GBP/USD"],
            "local_caching_path": "data/mock_data/spot",
            "seed_data_path": "seed_data.json",
            "num_records": 1000
          },
          {
            "profile_name": "alphavantage_spot_fx_batch",
            "api_type": "alphavantage",
            "tickers": ["EUR/USD", "USD/JPY", "GBP/USD"],
            "remote_caching": {
              "storage_account_type": "azure",
              "storage_account_name": "your_storage_account",
              "storage_account_key": "your_storage_key",
              "container_name": "your_container",
              "blob_path": "path/to/asset_pricing/fx/spot"
            },
            "api_key": "YOUR_ALPHAVANTAGE_API_KEY",
            "base_url": "https://www.alphavantage.co/query",
            "polling_interval": 60
          }
        ]
      }
    }
  },
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "asset_pricing_db",
    "user": "your_username",
    "password": "your_password",
    "db_schema": "asset_pricing"
  }
}
```

Note that we include an implementation for retrieving historical FX prices from the Alpha Vantage API via the alphavantage_fx_batch profile, and to generate mock prices via the mocked_fx_batch profile.

- Poetry has to be installed. For example for OSX:
```
brew install pipx
pipx ensurepath
pipx install poetry
```

- A PostgreSQL db accessible to be used in a predefined schema.

- A spark cluster or locally configured one-node development environment (requires to point to the PostgreSQL JDBC driver and Hadoop Azure jars). If using a cluster replace the "master" configuration by something like spark://spark-master:7077.

- Enough local storage if not using a service storage.

- Optionally if not using local storage you have to configure a service storage in Azure whcih is the only implemented external storage option in this project so far (You can implement storage in other cloud providers or even a self managed hadoop cluster as the code is clearly allowing for that).

### Database Setup

To create the necessary database schema and tables for FX Spot rates, run the following command:

```
psql -h localhost -U your_username -d asset_pricing_db -f asset_pricing/fx/spot/sql/fx_spot.sql
```

## Building
Uses poetry for dependency management and packaging. 
```
poetry build
```

## Contributing
- To add dependencies use poetry as it will take care of adding them to pyproject.toml, for example:
```
poetry add requests pydantic pyspark azure-storage-blob
```
## Usage

### fx spot
The fetch process pulls data from a feed and stores it locally or in S3 depending on the configuration:
```
# Fetch using the Alpha Vantage API
poetry run fetch-fx-spot .config.json alphavantage_fx_batch
# Fetch using a mocked fx spot service
poetry run fetch-fx-spot .config.json mocked_fx_spot_batch 
```

To process the fx spot instruments we use spark in local single-node for development or cluster-mode for production. This is a very efficient an idempotent process as it doesn't read files that are not needed, does the timestamp comparison before loading any data, uses Spark-based processing for scalability, and leverages the file naming convention from the fetch script.
```
# Process Alpha Vantage API fetched data
poetry run process-fx-spot .config.json alphavantage_fx_batch
# Process mocked fx spot fetched data
poetry run process-fx-spot .config.json mocked_fx_spot_batch
```

To manually test that the fx spot rates are in the db:
```
select * from asset_pricing.fx_spot_rates;
```
