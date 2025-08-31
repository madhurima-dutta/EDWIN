# Vessel EUAs & FuelEU Maritime Penalty Calculator
## EDWIN (Emission Data & Web Integration)

A comprehensive Streamlit application for calculating European Union Allowances (EUAs) and FuelEU Maritime penalties for vessel operations in EU waters.

## Overview

This application helps maritime operators calculate:
- **Carbon Emissions** from fuel consumption (HFO, LFO, MGO, LNG)
- **EUAs (European Union Allowances)** based on EU port regulations
- **FuelEU Maritime Penalties** based on GHG intensity calculations
- **Compliance Metrics** for maritime environmental regulations

## Features

- **Interactive Dashboard**: Real-time calculations with date range selection
- **Vessel Management**: Select from available vessels in your fleet
- **Multi-Fuel Support**: Handles HFO, LFO, MGO, and LNG fuel types
- **Database Integration**: Connects to Supabase PostgreSQL database
- **Data Visualization**: Clear metrics display and detailed voyage tables
- **Export Functionality**: Save calculations back to database

##  Installation

### Prerequisites
- Python 3.8 or higher
- Supabase account with PostgreSQL database
- Required database tables (vessels, voyages, fuel consumption data)

### Setup Steps

1. **Clone or download the project files**
   ```bash
   # Ensure you have these files:
   - streamlit_app.py
   - requirements.txt
   - .env.example
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   # Copy the example file
   cp .env.example .env
   
   # Edit .env and add your database URL
   DATABASE_URL=postgresql://username:password@host:port/database
   ```

4. **Run the application**
   ```bash
   streamlit run streamlit_app.py
   ```

## Database Schema

The application expects the following database structure:

### Required Tables
- `vessels` - Vessel information and specifications
- `voyages` - Voyage records with dates and routes
- `fuel_consumption` - Daily fuel consumption data
- `ports` - Port information for EU/non-EU classification

### Key Columns
- Vessel details: name, IMO number, specifications
- Fuel consumption: HFO, LFO, MGO, LNG quantities
- Voyage data: departure/arrival dates, ports
- Emission factors and regulatory parameters

## Usage

1. **Select Vessel**: Choose from available vessels in the sidebar
2. **Set Date Range**: Use the date pickers to define analysis period
3. **View Results**: 
   - Key metrics displayed at the top (CO₂, EUAs, GHG intensity, penalties)
   - Detailed voyage data in expandable table
4. **Save Data**: Results are automatically saved to the database

## Calculations

### EUAs (European Union Allowances)
- Based on CO₂ emissions from fuel consumption in EU waters
- Calculated using official emission factors per fuel type
- Applied only to voyages involving EU ports

### FuelEU Maritime Penalties
- GHG intensity calculations based on fuel mix and consumption
- Compliance assessment against regulatory thresholds
- Penalty calculations for non-compliant operations

## Configuration

### Environment Variables
```env
DATABASE_URL=your_supabase_postgresql_url
```

### Fuel Emission Factors
The application uses standard maritime emission factors:
- HFO: 3.114 kg CO₂/kg fuel
- LFO: 3.151 kg CO₂/kg fuel  
- MGO: 3.206 kg CO₂/kg fuel
- LNG: 2.750 kg CO₂/kg fuel

## Troubleshooting

### Common Issues

**Database Connection Error**
- Verify DATABASE_URL in .env file
- Check network connectivity to Supabase
- Ensure database credentials are correct

**No Vessels Available**
- Verify vessels table has data
- Check database permissions
- Confirm table schema matches expected structure

**Calculation Errors**
- Ensure fuel consumption data is available for selected date range
- Verify voyage records exist for the vessel
- Check for missing port information

## License

This project is for maritime compliance calculations and environmental reporting.

## Support

For technical support or questions about maritime regulations:
- Check database connectivity and schema
- Verify fuel consumption data completeness
- Ensure voyage records cover the selected date range

---

**Suggested App URL**: `vessel-euas-calculator` (Not given for confidentiality purposes)
