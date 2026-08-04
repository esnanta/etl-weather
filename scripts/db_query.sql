CREATE SCHEMA IF NOT EXISTS weather_era5;

CREATE TABLE weather_era5.dataset
(
    id BIGSERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    downloaded_at TIMESTAMPTZ NOT NULL,
    transformed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE weather_era5.observation
(
    id BIGSERIAL PRIMARY KEY,
    dataset_id BIGINT NOT NULL
        REFERENCES weather_era5.dataset(id),

    observation_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    tp DOUBLE PRECISION,
    ssrd DOUBLE PRECISION
);

CREATE INDEX idx_observation_time
ON weather_era5.observation(observation_time);

CREATE INDEX idx_observation_location
ON weather_era5.observation(latitude, longitude);

CREATE INDEX idx_dataset
ON weather_era5.observation(dataset_id);