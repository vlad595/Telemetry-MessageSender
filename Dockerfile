FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["Telemetry-MessageSender.csproj", "./"]
RUN dotnet restore "Telemetry-MessageSender.csproj"

COPY . .
RUN dotnet publish "Telemetry-MessageSender.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.Microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Telemetry-MessageSender.dll"]
