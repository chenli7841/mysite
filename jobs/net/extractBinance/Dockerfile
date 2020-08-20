FROM mcr.microsoft.com/dotnet/core/aspnet:3.1
COPY ./bin/Release/netcoreapp3.1/publish/ App/
COPY ./credentials/bigquery.json App/credentials/bigquery.json
WORKDIR /App
ENTRYPOINT ["dotnet", "extractBinance.dll", "BTCUSDT"]
