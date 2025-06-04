import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime


class EmployeeDataProcessor:
    def __init__(self, df):
        self.df = df.copy()

    def remove_dupes_fill_salary(self):
        # Remove duplicates
        self.df = self.df.drop_duplicates()
        # Fill or drop missing values as appropriate
        self.df = self.df.dropna(subset=['name', 'position', 'start_date'])
        self.df['salary'] = self.df['salary'].fillna(self.df['salary'].median())
        return self

    def transform_data(self):
        # Convert start_date to datetime
        self.df['start_date'] = pd.to_datetime(self.df['start_date'], errors='coerce')
        # Standardize job titles (example: trimming, lower case)
        self.df['position'] = self.df['position'].str.strip().str.title()
        return self

    def convert_start_date_to_years(self):
        # Years of service
        today = pd.to_datetime(datetime.today().date())
        self.df['years_of_service'] = (today - self.df['start_date']).dt.days // 365
        return self

    def scale_numeric(self):
        # Scale salary using MinMaxScaler
        scaler = MinMaxScaler()
        if 'salary' in self.df.columns:
            self.df['salary_scaled'] = scaler.fit_transform(self.df[['salary']])
        return self

    def clean_and_process(self):
        return (
            self.remove_dupes_fill_salary()
            .transform_data()
            .convert_start_date_to_years()
            .scale_numeric()
            .df
        )

