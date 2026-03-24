import fetch from "node-fetch";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const CANONICAL_INDICATORS: CanonicalIndicator[] = [
  // OUTPUT & GROWTH
  { canonical_id: "GDP_CURRENT_USD",           name: "GDP (Current USD)",                          category: "output",      unit: "USD",     frequency: "annual",    description: "Gross domestic product in current US dollars" },
  { canonical_id: "GDP_CONSTANT_USD",          name: "GDP (Constant 2015 USD)",                    category: "output",      unit: "USD",     frequency: "annual",    description: "Gross domestic product in constant 2015 US dollars" },
  { canonical_id: "GDP_GROWTH_PCT",            name: "GDP Growth Rate",                            category: "output",      unit: "percent", frequency: "annual",    description: "Annual percentage growth of real GDP" },
  { canonical_id: "GDP_PER_CAPITA_USD",        name: "GDP per Capita (Current USD)",               category: "output",      unit: "USD",     frequency: "annual",    description: "GDP divided by midyear population" },
  { canonical_id: "GDP_PER_CAPITA_PPP",        name: "GDP per Capita (PPP, Int. USD)",             category: "output",      unit: "USD",     frequency: "annual",    description: "GDP per capita based on purchasing power parity" },
  { canonical_id: "GDP_PPP",                   name: "GDP (PPP, Int. USD)",                        category: "output",      unit: "USD",     frequency: "annual",    description: "GDP based on purchasing power parity in international dollars" },
  { canonical_id: "GDP_DEFLATOR",              name: "GDP Deflator",                               category: "output",      unit: "index",   frequency: "annual",    description: "GDP deflator index, base year varies" },
  { canonical_id: "GROSS_CAPITAL_FORMATION",   name: "Gross Capital Formation (% GDP)",            category: "output",      unit: "percent", frequency: "annual",    description: "Gross capital formation as a percentage of GDP" },
  { canonical_id: "GROSS_SAVINGS_PCT",         name: "Gross Savings (% GDP)",                      category: "output",      unit: "percent", frequency: "annual",    description: "Gross savings as a percentage of GDP" },
  { canonical_id: "OUTPUT_GAP",                name: "Output Gap (% Potential GDP)",               category: "output",      unit: "percent", frequency: "annual",    description: "Actual minus potential GDP as a percentage of potential GDP" },

  // PRICES & INFLATION
  { canonical_id: "CPI_ANNUAL_PCT",            name: "CPI Inflation (Annual)",                     category: "prices",      unit: "percent", frequency: "annual",    description: "Annual percentage change in consumer price index" },
  { canonical_id: "CPI_MONTHLY_PCT",           name: "CPI Inflation (Monthly)",                    category: "prices",      unit: "percent", frequency: "monthly",   description: "Month-on-month percentage change in consumer price index" },
  { canonical_id: "INFLATION_AVG_PCT",         name: "Inflation, Average Consumer Prices",         category: "prices",      unit: "percent", frequency: "annual",    description: "Average annual consumer price inflation" },
  { canonical_id: "INFLATION_EOY_PCT",         name: "Inflation, End of Year",                     category: "prices",      unit: "percent", frequency: "annual",    description: "End-of-year consumer price inflation" },
  { canonical_id: "CORE_INFLATION_PCT",        name: "Core Inflation Rate",                        category: "prices",      unit: "percent", frequency: "annual",    description: "Inflation excluding food and energy" },
  { canonical_id: "PPI_ANNUAL_PCT",            name: "Producer Price Index (Annual)",              category: "prices",      unit: "percent", frequency: "annual",    description: "Annual percentage change in producer price index" },
  { canonical_id: "FOOD_INFLATION_PCT",        name: "Food Price Inflation",                       category: "prices",      unit: "percent", frequency: "annual",    description: "Annual percentage change in food prices" },
  { canonical_id: "HOUSE_PRICE_INDEX",         name: "Residential Property Price Index",           category: "prices",      unit: "index",   frequency: "quarterly", description: "Real residential property prices index" },
  { canonical_id: "COMMODITY_PRICE_OIL",       name: "Crude Oil Price (USD/barrel)",               category: "prices",      unit: "USD",     frequency: "annual",    description: "Average crude oil price in US dollars per barrel" },

  // LABOR MARKET
  { canonical_id: "UNEMPLOYMENT_PCT",          name: "Unemployment Rate",                          category: "labor",       unit: "percent", frequency: "annual",    description: "Share of labor force that is unemployed" },
  { canonical_id: "UNEMPLOYMENT_MONTHLY",      name: "Unemployment Rate (Monthly)",                category: "labor",       unit: "percent", frequency: "monthly",   description: "Monthly unemployment rate" },
  { canonical_id: "YOUTH_UNEMPLOYMENT_PCT",    name: "Youth Unemployment Rate",                    category: "labor",       unit: "percent", frequency: "annual",    description: "Unemployment rate for population aged 15-24" },
  { canonical_id: "LABOR_FORCE_PARTICIPATION", name: "Labor Force Participation Rate",             category: "labor",       unit: "percent", frequency: "annual",    description: "Labor force as a percentage of total working-age population" },
  { canonical_id: "EMPLOYMENT_GROWTH_PCT",     name: "Employment Growth Rate",                     category: "labor",       unit: "percent", frequency: "annual",    description: "Annual percentage change in total employment" },
  { canonical_id: "WAGE_GROWTH_PCT",           name: "Nominal Wage Growth",                        category: "labor",       unit: "percent", frequency: "annual",    description: "Annual percentage change in nominal wages" },
  { canonical_id: "REAL_WAGE_GROWTH_PCT",      name: "Real Wage Growth",                           category: "labor",       unit: "percent", frequency: "annual",    description: "Annual percentage change in real wages" },

  // EXTERNAL SECTOR
  { canonical_id: "CURRENT_ACCOUNT_GDP",       name: "Current Account Balance (% GDP)",            category: "external",    unit: "percent", frequency: "annual",    description: "Current account balance as a percentage of GDP" },
  { canonical_id: "CURRENT_ACCOUNT_USD",       name: "Current Account Balance (USD)",              category: "external",    unit: "USD",     frequency: "annual",    description: "Current account balance in US dollars" },
  { canonical_id: "TRADE_BALANCE_USD",         name: "Trade Balance (USD)",                        category: "external",    unit: "USD",     frequency: "annual",    description: "Goods trade balance in US dollars" },
  { canonical_id: "EXPORTS_USD",               name: "Exports of Goods and Services (USD)",        category: "external",    unit: "USD",     frequency: "annual",    description: "Total exports in current US dollars" },
  { canonical_id: "IMPORTS_USD",               name: "Imports of Goods and Services (USD)",        category: "external",    unit: "USD",     frequency: "annual",    description: "Total imports in current US dollars" },
  { canonical_id: "EXPORTS_GROWTH_PCT",        name: "Export Volume Growth",                       category: "external",    unit: "percent", frequency: "annual",    description: "Annual percentage change in export volume" },
  { canonical_id: "IMPORTS_GROWTH_PCT",        name: "Import Volume Growth",                       category: "external",    unit: "percent", frequency: "annual",    description: "Annual percentage change in import volume" },
  { canonical_id: "FDI_NET_USD",               name: "Foreign Direct Investment, Net (USD)",       category: "external",    unit: "USD",     frequency: "annual",    description: "Net FDI inflows in current US dollars" },
  { canonical_id: "FDI_INFLOWS_USD",           name: "FDI Inflows (USD)",                          category: "external",    unit: "USD",     frequency: "annual",    description: "FDI inflows in current US dollars" },
  { canonical_id: "FDI_GDP_PCT",               name: "FDI Net Inflows (% GDP)",                    category: "external",    unit: "percent", frequency: "annual",    description: "Net FDI inflows as a percentage of GDP" },
  { canonical_id: "RESERVES_USD",              name: "Foreign Reserves (USD)",                     category: "external",    unit: "USD",     frequency: "monthly",   description: "Total reserves including gold in US dollars" },
  { canonical_id: "RESERVES_MONTHS_IMPORTS",   name: "Foreign Reserves (Months of Imports)",       category: "external",    unit: "months",  frequency: "annual",    description: "Total reserves in months of import coverage" },
  { canonical_id: "REMITTANCES_USD",           name: "Remittances Inflows (USD)",                  category: "external",    unit: "USD",     frequency: "annual",    description: "Personal remittances received in current US dollars" },
  { canonical_id: "REMITTANCES_GDP_PCT",       name: "Remittances (% GDP)",                        category: "external",    unit: "percent", frequency: "annual",    description: "Personal remittances received as a percentage of GDP" },
  { canonical_id: "EXTERNAL_DEBT_USD",         name: "External Debt, Total (USD)",                 category: "external",    unit: "USD",     frequency: "annual",    description: "Total external debt stocks in current US dollars" },
  { canonical_id: "EXTERNAL_DEBT_GDP_PCT",     name: "External Debt (% GDP)",                      category: "external",    unit: "percent", frequency: "annual",    description: "Total external debt stocks as a percentage of GNI" },
  { canonical_id: "TERMS_OF_TRADE",            name: "Terms of Trade Index",                       category: "external",    unit: "index",   frequency: "annual",    description: "Net barter terms of trade index (2015=100)" },

  // FISCAL
  { canonical_id: "GOVT_DEBT_GDP",             name: "Government Debt (% GDP)",                    category: "fiscal",      unit: "percent", frequency: "annual",    description: "General government gross debt as a percentage of GDP" },
  { canonical_id: "GOVT_DEBT_USD",             name: "Government Debt (USD)",                      category: "fiscal",      unit: "USD",     frequency: "annual",    description: "General government gross debt in US dollars" },
  { canonical_id: "GOVT_BALANCE_GDP",          name: "Government Balance (% GDP)",                 category: "fiscal",      unit: "percent", frequency: "annual",    description: "General government net lending/borrowing as a percentage of GDP" },
  { canonical_id: "GOVT_PRIMARY_BALANCE_GDP",  name: "Government Primary Balance (% GDP)",         category: "fiscal",      unit: "percent", frequency: "annual",    description: "Government balance excluding interest payments as a percentage of GDP" },
  { canonical_id: "GOVT_REVENUE_GDP",          name: "Government Revenue (% GDP)",                 category: "fiscal",      unit: "percent", frequency: "annual",    description: "General government revenue as a percentage of GDP" },
  { canonical_id: "GOVT_EXPENDITURE_GDP",      name: "Government Expenditure (% GDP)",             category: "fiscal",      unit: "percent", frequency: "annual",    description: "General government total expenditure as a percentage of GDP" },
  { canonical_id: "TAX_REVENUE_GDP",           name: "Tax Revenue (% GDP)",                        category: "fiscal",      unit: "percent", frequency: "annual",    description: "Tax revenue as a percentage of GDP" },
  { canonical_id: "INTEREST_PAYMENTS_GDP",     name: "Interest Payments on Debt (% GDP)",          category: "fiscal",      unit: "percent", frequency: "annual",    description: "Government interest payments as a percentage of GDP" },
  { canonical_id: "FISCAL_MULTIPLIER",         name: "Structural Fiscal Balance (% GDP)",          category: "fiscal",      unit: "percent", frequency: "annual",    description: "Cyclically adjusted fiscal balance as a percentage of potential GDP" },

  // MONETARY & FINANCIAL
  { canonical_id: "POLICY_RATE_PCT",           name: "Central Bank Policy Rate",                   category: "monetary",    unit: "percent", frequency: "monthly",   description: "Central bank benchmark interest rate" },
  { canonical_id: "MONEY_SUPPLY_M2_USD",       name: "Money Supply M2 (USD)",                      category: "monetary",    unit: "USD",     frequency: "monthly",   description: "Broad money supply M2 in US dollars" },
  { canonical_id: "MONEY_SUPPLY_M2_GROWTH",    name: "Money Supply M2 Growth (Annual)",            category: "monetary",    unit: "percent", frequency: "annual",    description: "Annual percentage change in broad money supply M2" },
  { canonical_id: "FX_RATE_USD",               name: "Exchange Rate (LCU per USD)",                category: "monetary",    unit: "LCU",     frequency: "annual",    description: "Official exchange rate local currency units per US dollar" },
  { canonical_id: "FX_RATE_MONTHLY",           name: "Exchange Rate Monthly (LCU per USD)",        category: "monetary",    unit: "LCU",     frequency: "monthly",   description: "Monthly average exchange rate local currency units per US dollar" },
  { canonical_id: "REAL_EFFECTIVE_FX",         name: "Real Effective Exchange Rate",               category: "monetary",    unit: "index",   frequency: "monthly",   description: "Real effective exchange rate index (2020=100)" },
  { canonical_id: "LENDING_RATE_PCT",          name: "Bank Lending Rate",                          category: "monetary",    unit: "percent", frequency: "annual",    description: "Lending interest rate charged by banks on loans to prime customers" },
  { canonical_id: "DEPOSIT_RATE_PCT",          name: "Bank Deposit Rate",                          category: "monetary",    unit: "percent", frequency: "annual",    description: "Interest rate paid by commercial banks on deposits" },
  { canonical_id: "REAL_INTEREST_RATE_PCT",    name: "Real Interest Rate",                         category: "monetary",    unit: "percent", frequency: "annual",    description: "Lending rate adjusted for inflation as measured by the GDP deflator" },
  { canonical_id: "TREASURY_10Y_YIELD",        name: "10-Year Government Bond Yield",              category: "monetary",    unit: "percent", frequency: "monthly",   description: "10-year government bond yield" },
  { canonical_id: "CREDIT_GDP",                name: "Private Credit (% GDP)",                     category: "monetary",    unit: "percent", frequency: "annual",    description: "Domestic credit to private sector as a percentage of GDP" },
  { canonical_id: "CREDIT_GROWTH_PCT",         name: "Private Credit Growth",                      category: "monetary",    unit: "percent", frequency: "annual",    description: "Annual percentage change in domestic credit to private sector" },

  // FINANCIAL SECTOR
  { canonical_id: "NPL_RATIO_PCT",             name: "Non-Performing Loans Ratio",                 category: "financial",   unit: "percent", frequency: "annual",    description: "Bank non-performing loans as a percentage of total loans" },
  { canonical_id: "BANK_CAPITAL_RATIO",        name: "Bank Capital to Assets Ratio",               category: "financial",   unit: "percent", frequency: "annual",    description: "Ratio of bank capital and reserves to total assets" },
  { canonical_id: "STOCK_MARKET_CAP_GDP",      name: "Stock Market Capitalization (% GDP)",        category: "financial",   unit: "percent", frequency: "annual",    description: "Stock market capitalization as a percentage of GDP" },
  { canonical_id: "STOCK_MARKET_RETURN",       name: "Stock Market Total Return",                  category: "financial",   unit: "percent", frequency: "annual",    description: "Annual total return of the domestic stock market" },
  { canonical_id: "CREDIT_GAP",                name: "Credit-to-GDP Gap",                          category: "financial",   unit: "percent", frequency: "quarterly", description: "Deviation of credit-to-GDP ratio from its long-run trend" },
  { canonical_id: "DEBT_SERVICE_RATIO",        name: "Debt Service Ratio (Private Non-Financial)", category: "financial",   unit: "percent", frequency: "quarterly", description: "Debt service costs as a share of income for private non-financial sector" },

  // DEMOGRAPHIC
  { canonical_id: "POPULATION",                name: "Population",                                 category: "demographic", unit: "persons", frequency: "annual",    description: "Total population" },
  { canonical_id: "POPULATION_GROWTH_PCT",     name: "Population Growth Rate",                     category: "demographic", unit: "percent", frequency: "annual",    description: "Annual population growth rate" },
  { canonical_id: "URBAN_POPULATION_PCT",      name: "Urban Population (% of Total)",              category: "demographic", unit: "percent", frequency: "annual",    description: "Urban population as a percentage of total population" },
  { canonical_id: "LIFE_EXPECTANCY",           name: "Life Expectancy at Birth",                   category: "demographic", unit: "years",   frequency: "annual",    description: "Life expectancy at birth in years" },
  { canonical_id: "FERTILITY_RATE",            name: "Total Fertility Rate",                       category: "demographic", unit: "births",  frequency: "annual",    description: "Average number of children per woman" },
  { canonical_id: "DEPENDENCY_RATIO",          name: "Age Dependency Ratio",                       category: "demographic", unit: "percent", frequency: "annual",    description: "Dependents as a percentage of working-age population" },

  // ENERGY & ENVIRONMENT
  { canonical_id: "ENERGY_USE_PER_CAPITA",     name: "Energy Use per Capita (kg oil equiv.)",      category: "energy",      unit: "kg",      frequency: "annual",    description: "Energy use per capita in kilograms of oil equivalent" },
  { canonical_id: "ELECTRICITY_ACCESS_PCT",    name: "Access to Electricity (% Population)",       category: "energy",      unit: "percent", frequency: "annual",    description: "Population with access to electricity" },
  { canonical_id: "RENEWABLE_ENERGY_PCT",      name: "Renewable Energy (% Total Energy)",          category: "energy",      unit: "percent", frequency: "annual",    description: "Renewable energy consumption as a percentage of total final energy" },
  { canonical_id: "CO2_EMISSIONS_PER_CAPITA",  name: "CO2 Emissions per Capita",                   category: "energy",      unit: "tonnes",  frequency: "annual",    description: "CO2 emissions in metric tonnes per capita" },

  // DEVELOPMENT & SOCIAL
  { canonical_id: "GNI_PER_CAPITA_ATLAS",      name: "GNI per Capita (Atlas Method, USD)",         category: "development", unit: "USD",     frequency: "annual",    description: "Gross national income per capita using Atlas method" },
  { canonical_id: "POVERTY_HEADCOUNT_PCT",     name: "Poverty Headcount Ratio ($2.15/day)",        category: "development", unit: "percent", frequency: "annual",    description: "Population living below $2.15 per day (2017 PPP)" },
  { canonical_id: "GINI_INDEX",                name: "Gini Index",                                 category: "development", unit: "index",   frequency: "annual",    description: "Gini index measuring income inequality (0=perfect equality, 100=perfect inequality)" },
  { canonical_id: "HUMAN_CAPITAL_INDEX",       name: "Human Capital Index",                        category: "development", unit: "index",   frequency: "annual",    description: "World Bank Human Capital Index (0 to 1)" },
  { canonical_id: "INTERNET_USERS_PCT",        name: "Internet Users (% Population)",              category: "development", unit: "percent", frequency: "annual",    description: "Individuals using the Internet as a percentage of population" },
  { canonical_id: "MOBILE_SUBSCRIPTIONS",      name: "Mobile Subscriptions (per 100 people)",      category: "development", unit: "per100",  frequency: "annual",    description: "Mobile cellular subscriptions per 100 people" },

  // TRADE STRUCTURE
  { canonical_id: "TRADE_OPENNESS",            name: "Trade Openness (% GDP)",                     category: "trade",       unit: "percent", frequency: "annual",    description: "Sum of exports and imports as a percentage of GDP" },
  { canonical_id: "EXPORTS_GOODS_PCT_GDP",     name: "Goods Exports (% GDP)",                      category: "trade",       unit: "percent", frequency: "annual",    description: "Merchandise exports as a percentage of GDP" },
  { canonical_id: "IMPORTS_GOODS_PCT_GDP",     name: "Goods Imports (% GDP)",                      category: "trade",       unit: "percent", frequency: "annual",    description: "Merchandise imports as a percentage of GDP" },
  { canonical_id: "SERVICES_EXPORTS_USD",      name: "Services Exports (USD)",                     category: "trade",       unit: "USD",     frequency: "annual",    description: "Commercial services exports in current US dollars" },
  { canonical_id: "SERVICES_IMPORTS_USD",      name: "Services Imports (USD)",                     category: "trade",       unit: "USD",     frequency: "annual",    description: "Commercial services imports in current US dollars" },
  { canonical_id: "TOURISM_RECEIPTS_USD",      name: "International Tourism Receipts (USD)",       category: "trade",       unit: "USD",     frequency: "annual",    description: "International tourism receipts in current US dollars" },

  // BANKING & CREDIT
  { canonical_id: "DOMESTIC_CREDIT_GDP",       name: "Domestic Credit (% GDP)",                    category: "financial",   unit: "percent", frequency: "annual",    description: "Domestic credit provided by financial sector as a percentage of GDP" },
  { canonical_id: "BROAD_MONEY_GDP",           name: "Broad Money (% GDP)",                        category: "monetary",    unit: "percent", frequency: "annual",    description: "Broad money (M2) as a percentage of GDP" },
  { canonical_id: "FINANCIAL_OPENNESS",        name: "Financial Openness Index",                   category: "financial",   unit: "index",   frequency: "annual",    description: "Chinn-Ito index measuring financial account openness" },
];

const SOURCE_MAPPINGS: SourceMapping[] = [
  // OUTPUT & GROWTH
  { canonical_id: "GDP_CURRENT_USD",           source: "IMF",        source_code: "NGDPD",                source_database: "WEO", priority: 1 },
  { canonical_id: "GDP_CURRENT_USD",           source: "WORLD_BANK", source_code: "NY.GDP.MKTP.CD",        source_database: "WDI", priority: 2 },
  { canonical_id: "GDP_CONSTANT_USD",          source: "WORLD_BANK", source_code: "NY.GDP.MKTP.KD",        source_database: "WDI", priority: 1 },
  { canonical_id: "GDP_CONSTANT_USD",          source: "IMF",        source_code: "NGDP_R",                source_database: "WEO", priority: 2 },
  { canonical_id: "GDP_GROWTH_PCT",            source: "IMF",        source_code: "NGDP_RPCH",             source_database: "WEO", priority: 1 },
  { canonical_id: "GDP_GROWTH_PCT",            source: "WORLD_BANK", source_code: "NY.GDP.MKTP.KD.ZG",     source_database: "WDI", priority: 2 },
  { canonical_id: "GDP_PER_CAPITA_USD",        source: "IMF",        source_code: "NGDPDPC",               source_database: "WEO", priority: 1 },
  { canonical_id: "GDP_PER_CAPITA_USD",        source: "WORLD_BANK", source_code: "NY.GDP.PCAP.CD",        source_database: "WDI", priority: 2 },
  { canonical_id: "GDP_PER_CAPITA_PPP",        source: "IMF",        source_code: "PPPPC",                 source_database: "WEO", priority: 1 },
  { canonical_id: "GDP_PER_CAPITA_PPP",        source: "WORLD_BANK", source_code: "NY.GDP.PCAP.PP.CD",     source_database: "WDI", priority: 2 },
  { canonical_id: "GDP_PPP",                   source: "IMF",        source_code: "PPPGDP",                source_database: "WEO", priority: 1 },
  { canonical_id: "GDP_PPP",                   source: "WORLD_BANK", source_code: "NY.GDP.MKTP.PP.CD",     source_database: "WDI", priority: 2 },
  { canonical_id: "GDP_DEFLATOR",              source: "WORLD_BANK", source_code: "NY.GDP.DEFL.ZS",        source_database: "WDI", priority: 1 },
  { canonical_id: "GROSS_CAPITAL_FORMATION",   source: "WORLD_BANK", source_code: "NE.GDI.TOTL.ZS",        source_database: "WDI", priority: 1 },
  { canonical_id: "GROSS_CAPITAL_FORMATION",   source: "IMF",        source_code: "NID_NGDP",              source_database: "WEO", priority: 2 },
  { canonical_id: "GROSS_SAVINGS_PCT",         source: "WORLD_BANK", source_code: "NY.GNS.ICTR.ZS",        source_database: "WDI", priority: 1 },
  { canonical_id: "GROSS_SAVINGS_PCT",         source: "IMF",        source_code: "NGSD_NGDP",             source_database: "WEO", priority: 2 },
  { canonical_id: "OUTPUT_GAP",                source: "IMF",        source_code: "NGAP_NPGDP",            source_database: "WEO", priority: 1 },

  // PRICES
  { canonical_id: "CPI_ANNUAL_PCT",            source: "IMF",        source_code: "PCPIPCH",               source_database: "WEO", priority: 1 },
  { canonical_id: "CPI_ANNUAL_PCT",            source: "WORLD_BANK", source_code: "FP.CPI.TOTL.ZG",        source_database: "WDI", priority: 2 },
  { canonical_id: "CPI_MONTHLY_PCT",           source: "FRED",       source_code: "CPIAUCSL",              source_database: null,  priority: 1 },
  { canonical_id: "INFLATION_AVG_PCT",         source: "IMF",        source_code: "PCPIEPCH",              source_database: "WEO", priority: 1 },
  { canonical_id: "INFLATION_EOY_PCT",         source: "IMF",        source_code: "PCPIE",                 source_database: "WEO", priority: 1 },
  { canonical_id: "PPI_ANNUAL_PCT",            source: "WORLD_BANK", source_code: "FP.WPI.TOTL",           source_database: "WDI", priority: 1 },
  { canonical_id: "FOOD_INFLATION_PCT",        source: "WORLD_BANK", source_code: "FP.CPI.TOTL",           source_database: "WDI", priority: 1 },
  { canonical_id: "HOUSE_PRICE_INDEX",         source: "BIS",        source_code: "PROP",                  source_database: "WS_SPP", priority: 1 },
  { canonical_id: "COMMODITY_PRICE_OIL",       source: "IMF",        source_code: "POILWTI",               source_database: "WEO", priority: 1 },

  // LABOR
  { canonical_id: "UNEMPLOYMENT_PCT",          source: "IMF",        source_code: "LUR",                   source_database: "WEO", priority: 1 },
  { canonical_id: "UNEMPLOYMENT_PCT",          source: "WORLD_BANK", source_code: "SL.UEM.TOTL.ZS",        source_database: "WDI", priority: 2 },
  { canonical_id: "UNEMPLOYMENT_MONTHLY",      source: "FRED",       source_code: "UNRATE",                source_database: null,  priority: 1 },
  { canonical_id: "YOUTH_UNEMPLOYMENT_PCT",    source: "WORLD_BANK", source_code: "SL.UEM.1524.ZS",        source_database: "WDI", priority: 1 },
  { canonical_id: "LABOR_FORCE_PARTICIPATION", source: "WORLD_BANK", source_code: "SL.TLF.ACTI.ZS",        source_database: "WDI", priority: 1 },
  { canonical_id: "EMPLOYMENT_GROWTH_PCT",     source: "WORLD_BANK", source_code: "SL.EMP.TOTL.SP.ZS",     source_database: "WDI", priority: 1 },
  { canonical_id: "WAGE_GROWTH_PCT",           source: "IMF",        source_code: "AW",                    source_database: "WEO", priority: 1 },
  { canonical_id: "REAL_WAGE_GROWTH_PCT",      source: "IMF",        source_code: "RW",                    source_database: "WEO", priority: 1 },

  // EXTERNAL
  { canonical_id: "CURRENT_ACCOUNT_GDP",       source: "IMF",        source_code: "BCA_NGDPD",             source_database: "WEO", priority: 1 },
  { canonical_id: "CURRENT_ACCOUNT_GDP",       source: "WORLD_BANK", source_code: "BN.CAB.XOKA.GD.ZS",     source_database: "WDI", priority: 2 },
  { canonical_id: "CURRENT_ACCOUNT_USD",       source: "IMF",        source_code: "BCA",                   source_database: "WEO", priority: 1 },
  { canonical_id: "CURRENT_ACCOUNT_USD",       source: "WORLD_BANK", source_code: "BN.CAB.XOKA.CD",        source_database: "WDI", priority: 2 },
  { canonical_id: "TRADE_BALANCE_USD",         source: "WORLD_BANK", source_code: "BG.GSR.NFSV.GD.ZS",     source_database: "WDI", priority: 1 },
  { canonical_id: "EXPORTS_USD",               source: "WORLD_BANK", source_code: "NE.EXP.GNFS.CD",        source_database: "WDI", priority: 1 },
  { canonical_id: "EXPORTS_USD",               source: "IMF",        source_code: "TX_G_USD",              source_database: "BOP", priority: 2 },
  { canonical_id: "IMPORTS_USD",               source: "WORLD_BANK", source_code: "NE.IMP.GNFS.CD",        source_database: "WDI", priority: 1 },
  { canonical_id: "IMPORTS_USD",               source: "IMF",        source_code: "TM_G_USD",              source_database: "BOP", priority: 2 },
  { canonical_id: "EXPORTS_GROWTH_PCT",        source: "IMF",        source_code: "TX_RPCH",               source_database: "WEO", priority: 1 },
  { canonical_id: "IMPORTS_GROWTH_PCT",        source: "IMF",        source_code: "TM_RPCH",               source_database: "WEO", priority: 1 },
  { canonical_id: "FDI_NET_USD",               source: "WORLD_BANK", source_code: "BX.KLT.DINV.CD.WD",     source_database: "WDI", priority: 1 },
  { canonical_id: "FDI_INFLOWS_USD",           source: "IMF",        source_code: "BFD_BP6_USD",           source_database: "BOP", priority: 1 },
  { canonical_id: "FDI_GDP_PCT",               source: "WORLD_BANK", source_code: "BX.KLT.DINV.WD.GD.ZS",  source_database: "WDI", priority: 1 },
  { canonical_id: "RESERVES_USD",              source: "IMF",        source_code: "RAXG",                  source_database: "IFS", priority: 1 },
  { canonical_id: "RESERVES_USD",              source: "WORLD_BANK", source_code: "FI.RES.TOTL.CD",        source_database: "WDI", priority: 2 },
  { canonical_id: "RESERVES_MONTHS_IMPORTS",   source: "WORLD_BANK", source_code: "FI.RES.TOTL.MO",        source_database: "WDI", priority: 1 },
  { canonical_id: "REMITTANCES_USD",           source: "WORLD_BANK", source_code: "BX.TRF.PWKR.CD.DT",     source_database: "WDI", priority: 1 },
  { canonical_id: "REMITTANCES_GDP_PCT",       source: "WORLD_BANK", source_code: "BX.TRF.PWKR.DT.GD.ZS",  source_database: "WDI", priority: 1 },
  { canonical_id: "EXTERNAL_DEBT_USD",         source: "WORLD_BANK", source_code: "DT.DOD.DECT.CD",        source_database: "WDI", priority: 1 },
  { canonical_id: "EXTERNAL_DEBT_GDP_PCT",     source: "WORLD_BANK", source_code: "DT.DOD.DECT.GN.ZS",     source_database: "WDI", priority: 1 },
  { canonical_id: "TERMS_OF_TRADE",            source: "WORLD_BANK", source_code: "TT.PRI.MRCH.XD.WD",     source_database: "WDI", priority: 1 },

  // FISCAL
  { canonical_id: "GOVT_DEBT_GDP",             source: "IMF",        source_code: "GGXWDG_NGDP",           source_database: "WEO", priority: 1 },
  { canonical_id: "GOVT_DEBT_GDP",             source: "WORLD_BANK", source_code: "GC.DOD.TOTL.GD.ZS",     source_database: "WDI", priority: 2 },
  { canonical_id: "GOVT_DEBT_USD",             source: "IMF",        source_code: "GGXWDG",                 source_database: "WEO", priority: 1 },
  { canonical_id: "GOVT_BALANCE_GDP",          source: "IMF",        source_code: "GGXCNL_NGDP",           source_database: "WEO", priority: 1 },
  { canonical_id: "GOVT_PRIMARY_BALANCE_GDP",  source: "IMF",        source_code: "GGXONLB_NGDP",          source_database: "WEO", priority: 1 },
  { canonical_id: "GOVT_REVENUE_GDP",          source: "IMF",        source_code: "GGR_NGDP",              source_database: "WEO", priority: 1 },
  { canonical_id: "GOVT_EXPENDITURE_GDP",      source: "IMF",        source_code: "GGX_NGDP",              source_database: "WEO", priority: 1 },
  { canonical_id: "TAX_REVENUE_GDP",           source: "WORLD_BANK", source_code: "GC.TAX.TOTL.GD.ZS",     source_database: "WDI", priority: 1 },
  { canonical_id: "INTEREST_PAYMENTS_GDP",     source: "IMF",        source_code: "GGXNL_NGDP",            source_database: "WEO", priority: 1 },
  { canonical_id: "FISCAL_MULTIPLIER",         source: "IMF",        source_code: "GGXCNL_NGDP",           source_database: "WEO", priority: 1 },

  // MONETARY
  { canonical_id: "POLICY_RATE_PCT",           source: "FRED",       source_code: "FEDFUNDS",              source_database: null,       priority: 1 },
  { canonical_id: "POLICY_RATE_PCT",           source: "BIS",        source_code: "POLICY_RATE",           source_database: "WS_CBPOL", priority: 2 },
  { canonical_id: "MONEY_SUPPLY_M2_USD",       source: "FRED",       source_code: "M2SL",                  source_database: null,       priority: 1 },
  { canonical_id: "MONEY_SUPPLY_M2_GROWTH",    source: "WORLD_BANK", source_code: "FM.LBL.BMNY.ZG",        source_database: "WDI",      priority: 1 },
  { canonical_id: "FX_RATE_USD",               source: "IMF",        source_code: "ENDA",                  source_database: "IFS",      priority: 1 },
  { canonical_id: "FX_RATE_USD",               source: "WORLD_BANK", source_code: "PA.NUS.FCRF",           source_database: "WDI",      priority: 2 },
  { canonical_id: "FX_RATE_MONTHLY",           source: "FRED",       source_code: "DEXUSEU",               source_database: null,       priority: 1 },
  { canonical_id: "REAL_EFFECTIVE_FX",         source: "BIS",        source_code: "EXRATE",                source_database: "WS_XRU",   priority: 1 },
  { canonical_id: "LENDING_RATE_PCT",          source: "WORLD_BANK", source_code: "FR.INR.LEND",           source_database: "WDI",      priority: 1 },
  { canonical_id: "DEPOSIT_RATE_PCT",          source: "WORLD_BANK", source_code: "FR.INR.DPST",           source_database: "WDI",      priority: 1 },
  { canonical_id: "REAL_INTEREST_RATE_PCT",    source: "WORLD_BANK", source_code: "FR.INR.RINR",           source_database: "WDI",      priority: 1 },
  { canonical_id: "TREASURY_10Y_YIELD",        source: "FRED",       source_code: "GS10",                  source_database: null,       priority: 1 },
  { canonical_id: "CREDIT_GDP",                source: "WORLD_BANK", source_code: "FS.AST.PRVT.GD.ZS",     source_database: "WDI",      priority: 1 },
  { canonical_id: "CREDIT_GDP",                source: "BIS",        source_code: "CREDIT_GDP",            source_database: "WS_TC_C",  priority: 2 },
  { canonical_id: "CREDIT_GROWTH_PCT",         source: "WORLD_BANK", source_code: "FS.AST.PRVT.GD.ZS",     source_database: "WDI",      priority: 1 },
  { canonical_id: "BROAD_MONEY_GDP",           source: "WORLD_BANK", source_code: "FM.LBL.BMNY.GD.ZS",     source_database: "WDI",      priority: 1 },

  // FINANCIAL
  { canonical_id: "NPL_RATIO_PCT",             source: "WORLD_BANK", source_code: "FB.AST.NPER.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "BANK_CAPITAL_RATIO",        source: "WORLD_BANK", source_code: "FB.BNK.CAPA.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "STOCK_MARKET_CAP_GDP",      source: "WORLD_BANK", source_code: "CM.MKT.LCAP.GD.ZS",     source_database: "WDI",      priority: 1 },
  { canonical_id: "CREDIT_GAP",                source: "BIS",        source_code: "CREDIT_GDP",            source_database: "WS_CREDIT_GAP", priority: 1 },
  { canonical_id: "DEBT_SERVICE_RATIO",        source: "BIS",        source_code: "PROP",                  source_database: "WS_DSR",   priority: 1 },
  { canonical_id: "DOMESTIC_CREDIT_GDP",       source: "WORLD_BANK", source_code: "FS.AST.DOMS.GD.ZS",     source_database: "WDI",      priority: 1 },

  // DEMOGRAPHIC
  { canonical_id: "POPULATION",                source: "WORLD_BANK", source_code: "SP.POP.TOTL",           source_database: "WDI",      priority: 1 },
  { canonical_id: "POPULATION",                source: "IMF",        source_code: "LP",                    source_database: "WEO",      priority: 2 },
  { canonical_id: "POPULATION_GROWTH_PCT",     source: "WORLD_BANK", source_code: "SP.POP.GROW",           source_database: "WDI",      priority: 1 },
  { canonical_id: "URBAN_POPULATION_PCT",      source: "WORLD_BANK", source_code: "SP.URB.TOTL.IN.ZS",     source_database: "WDI",      priority: 1 },
  { canonical_id: "LIFE_EXPECTANCY",           source: "WORLD_BANK", source_code: "SP.DYN.LE00.IN",        source_database: "WDI",      priority: 1 },
  { canonical_id: "FERTILITY_RATE",            source: "WORLD_BANK", source_code: "SP.DYN.TFRT.IN",        source_database: "WDI",      priority: 1 },
  { canonical_id: "DEPENDENCY_RATIO",          source: "WORLD_BANK", source_code: "SP.POP.DPND",           source_database: "WDI",      priority: 1 },

  // ENERGY
  { canonical_id: "ENERGY_USE_PER_CAPITA",     source: "WORLD_BANK", source_code: "EG.USE.PCAP.KG.OE",     source_database: "WDI",      priority: 1 },
  { canonical_id: "ELECTRICITY_ACCESS_PCT",    source: "WORLD_BANK", source_code: "EG.ELC.ACCS.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "RENEWABLE_ENERGY_PCT",      source: "WORLD_BANK", source_code: "EG.FEC.RNEW.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "CO2_EMISSIONS_PER_CAPITA",  source: "WORLD_BANK", source_code: "EN.ATM.CO2E.PC",        source_database: "WDI",      priority: 1 },

  // DEVELOPMENT
  { canonical_id: "GNI_PER_CAPITA_ATLAS",      source: "WORLD_BANK", source_code: "NY.GNP.PCAP.CD",        source_database: "WDI",      priority: 1 },
  { canonical_id: "POVERTY_HEADCOUNT_PCT",     source: "WORLD_BANK", source_code: "SI.POV.DDAY",           source_database: "WDI",      priority: 1 },
  { canonical_id: "GINI_INDEX",                source: "WORLD_BANK", source_code: "SI.POV.GINI",           source_database: "WDI",      priority: 1 },
  { canonical_id: "HUMAN_CAPITAL_INDEX",       source: "WORLD_BANK", source_code: "HD.HCI.OVRL",           source_database: "WDI",      priority: 1 },
  { canonical_id: "INTERNET_USERS_PCT",        source: "WORLD_BANK", source_code: "IT.NET.USER.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "MOBILE_SUBSCRIPTIONS",      source: "WORLD_BANK", source_code: "IT.CEL.SETS.P2",        source_database: "WDI",      priority: 1 },

  // TRADE STRUCTURE
  { canonical_id: "TRADE_OPENNESS",            source: "WORLD_BANK", source_code: "NE.TRD.GNFS.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "EXPORTS_GOODS_PCT_GDP",     source: "WORLD_BANK", source_code: "NE.EXP.GNFS.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "IMPORTS_GOODS_PCT_GDP",     source: "WORLD_BANK", source_code: "NE.IMP.GNFS.ZS",        source_database: "WDI",      priority: 1 },
  { canonical_id: "SERVICES_EXPORTS_USD",      source: "WORLD_BANK", source_code: "BX.GSR.NFSV.CD",        source_database: "WDI",      priority: 1 },
  { canonical_id: "SERVICES_IMPORTS_USD",      source: "WORLD_BANK", source_code: "BM.GSR.NFSV.CD",        source_database: "WDI",      priority: 1 },
  { canonical_id: "TOURISM_RECEIPTS_USD",      source: "WORLD_BANK", source_code: "ST.INT.RCPT.CD",        source_database: "WDI",      priority: 1 },
];

async function verifySourceCodesExist(mappings: SourceMapping[]): Promise<void> {
  const wbCodes = mappings
    .filter((m) => m.source === "WORLD_BANK")
    .map((m) => m.source_code)
    .slice(0, 5);

  for (const code of wbCodes) {
    const url = `https://api.worldbank.org/v2/indicator/${code}?format=json`;
    const res = await fetch(url);
    if (!res.ok) {
      console.warn(`  Warning: World Bank indicator ${code} returned ${res.status}`);
    }
  }
}

async function upsertIndicators(indicators: CanonicalIndicator[]): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const ind of indicators) {
      await client.query(
        `INSERT INTO indicators (canonical_id, name, category, unit, frequency, description)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (canonical_id) DO UPDATE SET
           name        = EXCLUDED.name,
           category    = EXCLUDED.category,
           unit        = EXCLUDED.unit,
           frequency   = EXCLUDED.frequency,
           description = EXCLUDED.description`,
        [ind.canonical_id, ind.name, ind.category, ind.unit, ind.frequency, ind.description]
      );
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function upsertSourceMappings(mappings: SourceMapping[]): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const m of mappings) {
      await client.query(
        `INSERT INTO indicator_source_map (canonical_id, source, source_code, source_database, priority)
         VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (canonical_id, source) DO UPDATE SET
           source_code     = EXCLUDED.source_code,
           source_database = EXCLUDED.source_database,
           priority        = EXCLUDED.priority`,
        [m.canonical_id, m.source, m.source_code, m.source_database, m.priority]
      );
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function bootstrapIndicators(): Promise<void> {
  console.log("Seeding canonical indicators...");
  await upsertIndicators(CANONICAL_INDICATORS);
  console.log(`Seeded ${CANONICAL_INDICATORS.length} indicators.`);

  console.log("Verifying sample source codes against World Bank API...");
  await verifySourceCodesExist(SOURCE_MAPPINGS);

  console.log("Seeding source mappings...");
  await upsertSourceMappings(SOURCE_MAPPINGS);
  console.log(`Seeded ${SOURCE_MAPPINGS.length} source mappings.`);
}

interface CanonicalIndicator {
  canonical_id: string;
  name:         string;
  category:     string;
  unit:         string;
  frequency:    "annual" | "quarterly" | "monthly";
  description:  string;
}

interface SourceMapping {
  canonical_id:    string;
  source:          "IMF" | "WORLD_BANK" | "FRED" | "BIS" | "OECD";
  source_code:     string;
  source_database: string | null;
  priority:        number;
}