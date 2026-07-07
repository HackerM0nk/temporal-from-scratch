-- This script runs when the PostgreSQL container first starts.
-- The `temporal` and `temporal_visibility` databases are created automatically
-- by the temporalio/auto-setup image. We only need to create our DIY database.
CREATE DATABASE diy_workflows;
