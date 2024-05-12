#!/bin/bash

# Create README files
echo "Raw data files (CSV, Excel, etc.)" > ./data/raw_data/README.txt
echo "Processed data files (cleaned, preprocessed, feature-engineered)" > ./data/processed_data/README.txt
echo "Trained ML models (saved models, model checkpoints)" > ./backend/models/README.txt
echo "Backend scripts (data preprocessing, model training, evaluation)" > ./backend/scripts/README.txt
echo "Utility functions for backend tasks" > ./backend/utils/README.txt
echo "HTML templates (if using a web interface)" > ./frontend/templates/README.txt
echo "Static files (CSS, JavaScript, images)" > ./frontend/static/README.txt
echo "Frontend components" > ./frontend/components/README.txt
echo "Additional scripts for miscellaneous tasks" > ./helper_scripts/README.txt
echo "General utility functions used across the project" > ./utilities/README.txt
echo "Jupyter Notebooks" > ./notebooks/README.txt
echo "Any media files" > ./media/README.txt