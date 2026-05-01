-- Auto Generated (Do not modify) 0F54721B363710821E2B120EF0B15FE5B115DF0D151C844FA67FCF7F7BC9EBBC
CREATE VIEW [dbo].[vwAggrgate] AS (select [$Table].[CustomerKey] as [CustomerKey],
    [$Table].[CustomerAltKey] as [CustomerAltKey],
    [$Table].[Title] as [Title],
    [$Table].[FirstName] as [FirstName],
    [$Table].[LastName] as [LastName],
    [$Table].[AddressLine1] as [AddressLine1],
    [$Table].[City] as [City],
    [$Table].[StateProvince] as [StateProvince],
    [$Table].[CountryRegion] as [CountryRegion],
    [$Table].[PostalCode] as [PostalCode]
from [MyWarehouse].[dbo].[DimCustomer] as [$Table])