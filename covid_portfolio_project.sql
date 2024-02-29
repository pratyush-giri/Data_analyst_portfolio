use portfolioproject;
CREATE TABLE CovidDeath (
    iso_code VARCHAR(255) NULL,
    continent VARCHAR(255) NULL,
    location VARCHAR(255) NULL,
    date DATE NULL,
    population BIGINT NULL,
    total_cases VARCHAR(255) NULL,
    new_cases BIGINT NULL,
    new_cases_smoothed BIGINT NULL,
    total_deaths BIGINT NULL,
    new_deaths BIGINT NULL,
    new_deaths_smoothed BIGINT NULL,
    total_cases_per_million FLOAT NULL,
    new_cases_per_million FLOAT NULL,
    new_cases_smoothed_per_million FLOAT NULL,
    total_deaths_per_million FLOAT NULL,
    new_deaths_per_million FLOAT NULL,
    new_deaths_smoothed_per_million FLOAT NULL,
    reproduction_rate FLOAT NULL,
    icu_patients BIGINT NULL,
    icu_patients_per_million FLOAT NULL,
    hosp_patients BIGINT NULL,
    hosp_patients_per_million FLOAT NULL,
    weekly_icu_admissions BIGINT NULL,
    weekly_icu_admissions_per_million FLOAT NULL,
    weekly_hosp_admissions BIGINT NULL,
    weekly_hosp_admissions_per_million FLOAT NULL
);

load data infile 'covid_deaths_final.csv' into table CovidDeath
FIELDS TERMINATED BY ','
ignore 1 lines;

CREATE TABLE covidVaccination (
    iso_code VARCHAR(255),
    continent VARCHAR(255),
    location VARCHAR(255),
    date DATE,
    total_tests BIGINT,
    new_tests BIGINT,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    new_tests_smoothed BIGINT,
    new_tests_smoothed_per_thousand FLOAT,
    positive_rate FLOAT,
    tests_per_case FLOAT,
    tests_units VARCHAR(50),
    total_vaccinations BIGINT,
    people_vaccinated BIGINT,
    people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
    new_vaccinations_smoothed BIGINT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred FLOAT,
    people_fully_vaccinated_per_hundred FLOAT,
    total_boosters_per_hundred FLOAT,
    new_vaccinations_smoothed_per_million FLOAT,
    new_people_vaccinated_smoothed BIGINT,
    new_people_vaccinated_smoothed_per_hundred FLOAT,
    stringency_index FLOAT,
    population_density FLOAT,
    median_age FLOAT,
    aged_65_older FLOAT,
    aged_70_older FLOAT,
    gdp_per_capita FLOAT,
    extreme_poverty FLOAT,
    cardiovasc_death_rate FLOAT,
    diabetes_prevalence FLOAT,
    female_smokers FLOAT,
    male_smokers FLOAT,
    handwashing_facilities FLOAT,
    hospital_beds_per_thousand FLOAT,
    life_expectancy FLOAT,
    human_development_index FLOAT,
    excess_mortality_cumulative_absolute FLOAT,
    excess_mortality_cumulative FLOAT,
    excess_mortality FLOAT,
    excess_mortality_cumulative_per_million FLOAT
);
load data infile 'covid_vaccinations_final.csv' into table covidVaccination
FIELDS TERMINATED BY ','
ignore 1 lines;

select * 
from portfolioproject.coviddeath
order by 3,4

select *
from portfolioproject.covidvaccination


select Location,date,total_cases,new_cases,total_deaths,population
from coviddeath

-- total cases vs total deaths
select Location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 as death_percentage
from coviddeath
where location = 'India'
order by 1,2

-- population gotten covid
select Location,date,total_cases,population,(total_cases/population)*100 as infected_percentage
from coviddeath
order by 5 desc

-- countries with highest infection rate 
select Location,population,MAX(total_cases) as highest_infection_count ,MAX((total_cases/population))*100 as infection_rate
from coviddeath
group by Location, population
-- order by 4 desc

-- countries with highest death count per population
select Location ,Max(Total_deaths) as totaldeathcount
from coviddeath
group by Location
order by totaldeathcount desc

-- continent with highest death count per population

SELECT 
    continent,
    SUM(max_total_deaths) AS total_deaths_per_continent
FROM (
    SELECT 
        continent,
        Location,
        MAX(total_deaths) AS max_total_deaths
    FROM 
        coviddeath
    GROUP BY 
        continent,
        Location
) AS subquery
GROUP BY 
    continent;
    
-- global numbers
select date ,sum(new_cases) as totalCases,sum(new_deaths) as totalDeaths,sum(new_deaths)/sum(new_cases)*100 as deathPercentage
from coviddeath
group by date
order by 3 desc


-- total cases vs total deaths
select sum(new_cases) as totalCases,sum(new_deaths) as totalDeaths,sum(new_deaths)/sum(new_cases)*100 as deathPercentage
from coviddeath
order by 3 desc

-- taking a different course of action
select  dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations_smoothed
from coviddeath dea
join covidvaccination vac
	on dea.Location = vac.Location and dea.date = vac.date
order by 2,3

-- running vaccination count vs population 
select  dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations_smoothed,
sum(vac.new_vaccinations_smoothed) over (partition by dea.location order by dea.date) as rolling_vaccinations_count
from coviddeath dea
join covidvaccination vac
	on dea.Location = vac.Location and dea.date = vac.date
order by 2,3

-- by using cte 

with PopvsVac(Continent,Location,Date,Population,NewVaccination,RollingPeopleVaccinated)
as
(
select  dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations_smoothed,
sum(vac.new_vaccinations_smoothed) over (partition by dea.location order by dea.date) as rolling_vaccinations_count
from coviddeath dea
join covidvaccination vac
	on dea.Location = vac.Location and dea.date = vac.date
order by 2,3
)
select *,(RollingPeopleVaccinated/Population)*100 as VaccinationPercent
from PopvsVac
where Location = "India"
-- at the end of year 2021 100% people were vaccinated and at the end of 2023 155% of people were vaccinated


-- creating view for visualization
percenntpopulationvaccinatedcreate view percenntPopulationVaccinated as
select  dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations_smoothed,
sum(vac.new_vaccinations_smoothed) over (partition by dea.location order by dea.date) as rolling_vaccinations_count
from coviddeath dea
join covidvaccination vac
	on dea.Location = vac.Location and dea.date = vac.date
