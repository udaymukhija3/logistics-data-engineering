create extension if not exists postgis;

create schema if not exists reference;
create schema if not exists observability;

create table if not exists reference.hubs (
    hub_id text primary key,
    hub_name text not null,
    city text not null,
    hub_type text not null,
    latitude double precision not null,
    longitude double precision not null,
    created_at timestamptz default now()
);

create table if not exists observability.pipeline_runs (
    run_id bigserial primary key,
    pipeline_name text not null,
    run_status text not null,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    metadata jsonb default '{}'::jsonb
);
