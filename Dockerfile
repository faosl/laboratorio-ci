FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Ordering.API/Ordering.API.csproj Ordering.API/
RUN dotnet restore Ordering.API/Ordering.API.csproj
COPY Ordering.API/ Ordering.API/
RUN dotnet publish Ordering.API/Ordering.API.csproj -c Release -o /app/publish --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
COPY --from=build /app/publish .
USER app
ENTRYPOINT ["dotnet", "Ordering.API.dll"]
