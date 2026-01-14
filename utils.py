"""
Utility functions for data loading, risk calculation, and summary generation
"""

import pandas as pd
import json
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from prompts import (
    DATA_QUERY_CLASSIFICATION_PROMPT,
    DATA_QUERY_PLANNING_PROMPT,
    DATA_QUERY_RESPONSE_PROMPT,
    INTENT_CLASSIFICATION_PROMPT,
    ANALYSIS_SUMMARY_PROMPT
)
from extract_pdf_fields import extract_pdf_form_fields


@dataclass
class RiskScores:
    """Container for individual risk scores"""
    property_risk: float
    claims_risk: float
    geographic_risk: float
    protection_risk: float
    overall_score: float
    risk_level: str
    recommendation: str
    top_factors: List[str]
    property_factors: List[str]
    claims_factors: List[str]
    geographic_factors: List[str]
    protection_factors: List[str]


# =============================================================================
# RISK SCORING RULES
# =============================================================================

CONSTRUCTION_RISK = {
    "Fire Resistive": 10,
    "Non-Combustible": 25,
    "Masonry Non-Combustible": 30,
    "Joisted Masonry": 50,
    "Frame": 80,
}

ROOF_CONDITION_RISK = {
    "New": 10,
    "Very Good": 20,
    "Good": 30,
    "Fair": 50,
    "Poor": 80,
}

FEMA_FLOOD_ZONE_RISK = {
    "X": 10,
    "D": 20,
    "A": 50,
    "AE": 60,
    "VE": 90,
}

EARTHQUAKE_ZONE_RISK = {
    "Zone 0": 10,
    "Zone 1": 25,
    "Zone 2": 45,
    "Zone 3": 65,
    "Zone 4": 85,
}

BURGLAR_ALARM_RISK = {
    "Central Station": 10,
    "Video Verified": 20,
    "Monitored": 35,
    "Local": 50,
    "None": 80,
}


# =============================================================================
# DATA LOADING
# =============================================================================


def load_file_content(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Helper to load file content as CSV or Excel with fallbacks"""
    errors = []
    
    # Try reading based on extension first
    if file_path.lower().endswith('.csv'):
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            errors.append(f"CSV read failed: {e}")
            # Fallback to Excel
            try:
                return pd.read_excel(file_path, sheet_name=sheet_name if sheet_name else 0, engine='openpyxl')
            except Exception as e2:
                errors.append(f"Excel fallback failed: {e2}")
                
    else:
        # Default to Excel for non-csv extensions
        try:
            return pd.read_excel(file_path, sheet_name=sheet_name if sheet_name else 0, engine='openpyxl')
        except Exception as e:
            errors.append(f"Excel read failed: {e}")
            # Fallback to pure read_excel (let pandas decide engine) or CSV
            try:
                return pd.read_excel(file_path, sheet_name=sheet_name if sheet_name else 0)
            except Exception as e2:
                errors.append(f"Excel default engine failed: {e2}")
                # Fallback to CSV
                try:
                    return pd.read_csv(file_path)
                except Exception as e3:
                    errors.append(f"CSV fallback failed: {e3}")

    raise ValueError(f"Could not load file. Errors: {'; '.join(errors)}")


def load_property_data(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Load property data from CSV or Excel file"""
    return load_file_content(file_path, sheet_name)


def load_claims_data(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Load claims history from CSV or Excel file"""
    return load_file_content(file_path, sheet_name)


def detect_data_type(df: pd.DataFrame) -> str:
    """Analyze columns to determine if data is Property or Claims data"""
    cols = [str(c).lower() for c in df.columns]
    
    # Keywords specific to claims
    claims_keywords = ['claim', 'loss', 'accident', 'injury', 'reserve', 'payment', 'incurred']
    claims_score = sum(1 for k in claims_keywords if any(k in c for c in cols))
    
    # Keywords specific to property/SOV
    property_keywords = ['construction', 'tiv', 'sq ft', 'square feet', 'year built', 'sprinkler', 'roof', 'address', 'bpp', 'building']
    property_score = sum(1 for k in property_keywords if any(k in c for c in cols))
    
    if claims_score > property_score:
        return 'claims'
    elif property_score > claims_score:
        return 'property'
    return 'unknown'


def smart_load_data(file_path: str) -> Dict[str, pd.DataFrame]:
    """
    Smartly load data from a file.
    - Handles CSV, Excel, and PDF
    - For PDF: Extracts form fields and converts to DataFrame
    - Detects if Excel has multiple sheets for Claims vs Property
    - Returns dictionary with keys 'property_df' and/or 'claims_df'
    """
    result = {}
    
    # Check if file is a PDF
    if file_path.lower().endswith('.pdf'):
        try:
            print(f"Detected PDF file, extracting form fields...")
            extracted_data = extract_pdf_form_fields(file_path)
            
            if not extracted_data or all(not v for v in extracted_data.values()):
                raise ValueError("No data could be extracted from PDF")
            
            # Convert extracted data to DataFrame
            # Loss History is a list of dicts, we'll keep it as JSON string in the DataFrame
            df_data = extracted_data.copy()
            
            # Pre-calculate Loss Types for the property record if they exist in history
            if 'Loss History' in extracted_data and isinstance(extracted_data['Loss History'], list):
                loss_history = extracted_data['Loss History']
                types = set(entry.get('Type', '') for entry in loss_history if entry.get('Type'))
                if types:
                    df_data['Loss History - Type'] = ", ".join(types)
            
            # Convert Loss History array to JSON string for DataFrame storage
            if 'Loss History' in df_data and isinstance(df_data['Loss History'], list):
                df_data['Loss History'] = json.dumps(df_data['Loss History'])
            
            # Create DataFrame with single row
            df = pd.DataFrame([df_data])
            
            # PDF files typically contain property data
            result['property_df'] = df
            
            # Extract claims from Loss History if present
            if 'Loss History' in extracted_data and extracted_data['Loss History']:
                loss_history = extracted_data['Loss History']
                if isinstance(loss_history, list) and len(loss_history) > 0:
                    # Create claims DataFrame from loss history
                    claims_data = []
                    for loss_entry in loss_history:
                        # Add reference fields to link claims to property
                        claim_record = loss_entry.copy()
                        claim_record['Property'] = extracted_data.get('Named Insured', '')
                        claim_record['Agency Customer ID'] = extracted_data.get('Agency Customer ID', '')
                        claim_record['Street Address'] = extracted_data.get('Street Address', '')
                        claims_data.append(claim_record)
                    
                    if claims_data:
                        result['claims_df'] = pd.DataFrame(claims_data)
            
            print(f"✓ Successfully extracted PDF data: {len([v for v in extracted_data.values() if v])} fields populated")
            return result
            
        except Exception as e:
            print(f"PDF extraction failed: {e}")
            raise ValueError(f"Could not extract data from PDF file: {str(e)}")
    
    # First, try to open as Excel to check sheets (if it is an Excel file)
    is_excel = False
    try:
        # Check if valid excel file
        xl = pd.ExcelFile(file_path, engine='openpyxl')
        is_excel = True
        sheet_names = xl.sheet_names
        
        # If multiple sheets, check names
        claims_sheet = None
        property_sheet = None
        
        for sheet in sheet_names:
            sheet_lower = sheet.lower()
            if 'claim' in sheet_lower or 'loss' in sheet_lower:
                claims_sheet = sheet
            elif 'property' in sheet_lower or 'sov' in sheet_lower or 'loc' in sheet_lower or 'sched' in sheet_lower:
                property_sheet = sheet
        
        # If we found specific sheets, load them
        if claims_sheet:
            result['claims_df'] = pd.read_excel(file_path, sheet_name=claims_sheet, engine='openpyxl')
        if property_sheet:
            result['property_df'] = pd.read_excel(file_path, sheet_name=property_sheet, engine='openpyxl')
            
        # If no specific sheets detected via name, but it is Excel
        if not result:
            # If single sheet, load and detect
            if len(sheet_names) == 1:
                df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')
                dtype = detect_data_type(df)
                if dtype == 'claims':
                    result['claims_df'] = df
                else:
                    result['property_df'] = df
            else:
                # Multiple sheets but no clear names? Load first two and check?
                # For now, just load first sheet
                df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')
                dtype = detect_data_type(df)
                if dtype == 'claims':
                    result['claims_df'] = df
                else:
                    result['property_df'] = df
                    
    except Exception:
        # Not a valid Excel file or openpyxl failed -> Try CSV or flat load
        try:
            df = load_file_content(file_path)
            dtype = detect_data_type(df)
            if dtype == 'claims':
                result['claims_df'] = df
            else:
                result['property_df'] = df
        except Exception as e:
            print(f"Smart load failed: {e}")
            raise ValueError(f"Could not load data from file. Please check format.")

    return result


# =============================================================================
# RISK CALCULATION FUNCTIONS
# =============================================================================

def safe_float(val, default=0.0):
    """Safely convert value to float, handling strings and cleaning currency/percent symbols"""
    try:
        if pd.isna(val) or val == '':
            return default
        
        # Handle string inputs (including pypdf objects which behave like strings)
        val_str = str(val).strip()
        
        # Remove common non-numeric chars
        val_str = val_str.replace('$', '').replace(',', '').replace('%', '')
        
        return float(val_str)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    """Safely convert value to int"""
    try:
        return int(safe_float(val, default))
    except:
        return default

def calculate_property_risk(row: pd.Series) -> Tuple[float, List[str]]:

    """Calculate property risk score based on construction, age, roof condition, sprinklers"""
    factors = []
    scores = []
    
    # Construction Type Risk
    construction = row.get('Construction Type', 'Frame')
    construction_score = CONSTRUCTION_RISK.get(construction, 50)
    scores.append(construction_score)
    if construction_score >= 60:
        factors.append(f"Construction Type: {construction} (High Risk)")
    
    # Building Age Risk
    year_built = row.get('Year Built')
    if pd.isna(year_built) or year_built == '':
        year_built = 1970
    else:
        year_built = safe_int(year_built, 1970)
        
    current_year = 2025
    age = current_year - year_built
    if age > 50:
        age_score = 80
        factors.append(f"Building Age: {age} years (High Risk)")
    elif age > 25:
        age_score = 50
    else:
        age_score = 20
    scores.append(age_score)
    
    # Roof Condition Risk
    roof = row.get('Verified Roof Condition', 'Fair')
    roof_score = ROOF_CONDITION_RISK.get(roof, 50)
    scores.append(roof_score)
    if roof_score >= 60:
        factors.append(f"Roof Condition: {roof} (High Risk)")
    
    # Sprinkler Coverage Risk
    sprinkler_pct = safe_float(row.get('Sprinklered %'), 50.0)
    if sprinkler_pct > 70:
        sprinkler_score = 20
    elif sprinkler_pct > 30:
        sprinkler_score = 45
    else:
        sprinkler_score = 75
        factors.append(f"Low Sprinkler Coverage: {sprinkler_pct:.1f}%")
    scores.append(sprinkler_score)
    
    # Full breakdown
    breakdown = [
        f"**Construction Type:** {construction} ({construction_score}%)",
        f"**Year Built:** {year_built} (Age: {age} yrs, Score: {age_score}%)",
        f"**Roof Condition:** {roof} ({roof_score}%)",
        f"**Sprinkler Coverage:** {sprinkler_pct}% ({sprinkler_score}%)"
    ]
    
    return sum(scores) / len(scores), factors, breakdown


def calculate_claims_risk(row: pd.Series, claims_df: Optional[pd.DataFrame] = None) -> Tuple[float, List[str]]:
    """Calculate claims risk based on loss history"""
    factors = []
    scores = []
    
    # Initialize with values from property data summary (fallback)
    loss_count = safe_int(row.get('Loss History - Count'), 0)
    loss_amount = safe_float(row.get('Loss History - Total Amount'), 0.0)
    
    # If Claims Data is provided, try to calculate more accurate metrics
    if claims_df is not None and not claims_df.empty:
        matched_claims = pd.DataFrame()
        match_found = False
        
        # 1. Try matching by Agency Customer ID
        row_id = row.get('Agency Customer ID')
        if row_id and 'Agency Customer ID' in claims_df.columns:
            matched_claims = claims_df[claims_df['Agency Customer ID'] == row_id]
            if not matched_claims.empty:
                match_found = True
        
        # 2. Try matching by Address if no ID match
        if not match_found:
            row_addr = str(row.get('Street Address', '')).lower().strip()
            # Normalize common suffixes for better matching
            replacements = {
                'street': 'st', 'avenue': 'ave', 'road': 'rd', 'boulevard': 'blvd', 
                'drive': 'dr', 'place': 'pl', 'lane': 'ln', 'court': 'ct'
            }
            row_addr_norm = row_addr
            for full, abbr in replacements.items():
                row_addr_norm = row_addr_norm.replace(full, abbr).replace('.', '')
            
            if row_addr and row_addr != 'nan':
                # find address-like column in claims
                addr_cols = [c for c in claims_df.columns if 'address' in c.lower() or 'location' in c.lower()]
                for ac in addr_cols:
                    # Try exact sub-string match first
                    matches = claims_df[claims_df[ac].astype(str).str.lower().str.contains(row_addr, regex=False, na=False)]
                    
                    # If no match, try normalized match
                    if matches.empty:
                        # Create temporary normalized column for checking
                        temp_col = claims_df[ac].astype(str).str.lower()
                        for full, abbr in replacements.items():
                             temp_col = temp_col.str.replace(full, abbr).str.replace('.', '')
                        matches = claims_df[temp_col.str.contains(row_addr_norm, regex=False, na=False)]
                    
                    if not matches.empty:
                        matched_claims = pd.concat([matched_claims, matches])
                        match_found = True
                        break
        
        if match_found:
            calc_count = len(matched_claims)
            
            # Identify amount column
            # Prioritize: 'Total Incurred', 'Total Net Incurred', 'Loss Amount', etc.
            amount_cols = [c for c in claims_df.columns if any(k in c.lower() for k in ['incurred', 'paid', 'total amount', 'payment', 'claim amount'])]
            calc_amount = 0
            
            if amount_cols:
                # Use the first likely column containing financial data
                # Prefer 'Incurred' over 'Paid' as it represents total risk exposure
                target_col = next((c for c in amount_cols if 'incurred' in c.lower()), amount_cols[0])
                try:
                    # Clean currency strings if necessary (remove '$', ',')
                    if matched_claims[target_col].dtype == 'object':
                         calc_amount = matched_claims[target_col].replace(r'[\$,]', '', regex=True).astype(float).sum()
                    else:
                         calc_amount = matched_claims[target_col].sum()
                except:
                    pass
            
            # Use the calculated values (or max of both to be conservative/safe)
            # If the summary says 0 but we found claims, definitely use found claims.
            # If summary says 10 but we found 0 (due to bad matching), keeping summary is safer.
            loss_count = max(loss_count, calc_count)
            loss_amount = max(loss_amount, calc_amount)


    # Claim Count Risk
    if loss_count > 15:
        count_score = 90
        factors.append(f"High Claim Count: {loss_count} claims")
    elif loss_count > 5:
        count_score = 60
    elif loss_count > 2:
        count_score = 40
    else:
        count_score = 15
    scores.append(count_score)
    
    # Total Loss Amount Risk
    if loss_amount > 5000000:
        amount_score = 90
        factors.append(f"High Loss Amount: ${loss_amount:,.0f}")
    elif loss_amount > 2000000:
        amount_score = 65
    elif loss_amount > 500000:
        amount_score = 40
    else:
        amount_score = 20
    scores.append(amount_score)
    
    # Loss Type Risk (if available)
    loss_types_str = str(row.get('Loss History - Type', ''))
    if claims_df is not None and match_found and not matched_claims.empty:
        # aggregate types from claims - check multiple likely columns including "Loss Type"
        type_cols = [c for c in claims_df.columns if any(k in c.lower() for k in ['type', 'cause', 'reason', 'desc'])]
        collected_types = set()
        for col in type_cols:
             vals = matched_claims[col].dropna().astype(str).unique().tolist()
             collected_types.update(vals)
        if collected_types:
            loss_types_str = ", ".join(collected_types)
    
    if 'Fire' in loss_types_str:
        type_score = 80
        factors.append(f"Fire Loss History")
    elif 'Flood' in loss_types_str or 'Tornado' in loss_types_str:
        type_score = 70
    elif 'Theft' in loss_types_str or 'Vandalism' in loss_types_str:
        type_score = 40
    else:
        type_score = 30
    scores.append(type_score)
    
    return sum(scores) / len(scores), factors, [
        f"**Claim Count:** {loss_count} ({count_score}%)",
        f"**Total Loss Amount:** ${loss_amount:,.0f} ({amount_score}%)",
        f"**Loss Types:** {loss_types_str.strip() if loss_types_str.strip() else 'N/A'} ({type_score}%)"
    ]


def calculate_geographic_risk(row: pd.Series) -> Tuple[float, List[str]]:
    """Calculate geographic risk based on location factors"""
    factors = []
    scores = []
    
    # Wildfire Risk
    wildfire = safe_float(row.get('Wildfire Risk Score'), 50.0)
    if wildfire > 70:
        factors.append(f"High Wildfire Risk: {wildfire:.1f}")
    scores.append(wildfire)
    
    # FEMA Flood Zone
    flood_zone = row.get('FEMA Flood Zone', 'X')
    flood_score = FEMA_FLOOD_ZONE_RISK.get(flood_zone, 50)
    scores.append(flood_score)
    if flood_score >= 60:
        factors.append(f"FEMA Flood Zone: {flood_zone}")
    
    # Earthquake Zone
    eq_zone = row.get('Earthquake Zone', 'Zone 0')
    eq_score = EARTHQUAKE_ZONE_RISK.get(eq_zone, 30)
    scores.append(eq_score)
    if eq_score >= 60:
        factors.append(f"Earthquake Zone: {eq_zone}")
    
    # Crime Score
    crime = safe_float(row.get('Crime Score'), 50.0)
    if crime > 70:
        factors.append(f"High Crime Score: {crime:.1f}")
    scores.append(crime)
    
    return sum(scores) / len(scores), factors, [
        f"**Wildfire Risk:** {wildfire} ({wildfire}%)",
        f"**FEMA Flood Zone:** {flood_zone} ({flood_score}%)",
        f"**Earthquake Zone:** {eq_zone} ({eq_score}%)",
        f"**Crime Score:** {crime} ({crime}%)"
    ]


def calculate_protection_risk(row: pd.Series) -> Tuple[float, List[str]]:
    """Calculate protection risk based on safety systems"""
    factors = []
    scores = []
    
    # Fire Protection Class (1=best, 10=worst)
    fpc = safe_int(row.get('Fire Protection Class'), 5)
    if fpc >= 8:
        fpc_score = 80
        factors.append(f"Poor Fire Protection Class: {fpc}")
    elif fpc >= 5:
        fpc_score = 50
    else:
        fpc_score = 20
    scores.append(fpc_score)
    
    # Burglar Alarm Type
    alarm = row.get('Burglar Alarm Type', 'None')
    alarm_score = BURGLAR_ALARM_RISK.get(alarm, 50)
    scores.append(alarm_score)
    if alarm_score >= 60:
        factors.append(f"Burglar Alarm: {alarm}")
    
    # Distance to Fire Station
    # Check both keys
    dist_val = row.get('Distance to Fire Station (miles)')
    if pd.isna(dist_val) or dist_val == '':
        dist_val = row.get('Distance to Fire Station')
    
    distance = safe_float(dist_val, 10.0)
    if distance > 15:
        dist_score = 75
        factors.append(f"Far from Fire Station: {distance:.1f} mi")
    elif distance > 5:
        dist_score = 45
    else:
        dist_score = 20
    scores.append(dist_score)
    
    return sum(scores) / len(scores), factors, [
        f"**Fire Protection Class:** {fpc} ({fpc_score}%)",
        f"**Burglar Alarm Type:** {alarm} ({alarm_score}%)",
        f"**Fire Station Distance:** {distance:.1f} mi ({dist_score}%)"
    ]


def calculate_all_risk_scores(row: pd.Series, claims_df: Optional[pd.DataFrame] = None) -> RiskScores:
    """Calculate comprehensive risk scores for a property"""
    
    # Calculate individual risk categories
    # Calculate individual risk categories
    property_risk, property_notable, property_breakdown = calculate_property_risk(row)
    claims_risk, claims_notable, claims_breakdown = calculate_claims_risk(row, claims_df)
    geographic_risk, geo_notable, geo_breakdown = calculate_geographic_risk(row)
    protection_risk, protection_notable, protection_breakdown = calculate_protection_risk(row)
    
    # Weighted overall score
    weights = {
        'property': 0.25,
        'claims': 0.30,
        'geographic': 0.25,
        'protection': 0.20
    }
    
    overall_score = (
        property_risk * weights['property'] +
        claims_risk * weights['claims'] +
        geographic_risk * weights['geographic'] +
        protection_risk * weights['protection']
    )
    
    # Determine risk level and recommendation
    if overall_score < 45:
        risk_level = "LOW"
        recommendation = "AUTO-BIND ELIGIBLE"
    elif overall_score < 60:
        risk_level = "MEDIUM"
        recommendation = "STANDARD REVIEW"
    elif overall_score < 80:
        risk_level = "HIGH"
        recommendation = "REFER TO SENIOR UNDERWRITER"
    else:
        risk_level = "VERY HIGH"
        recommendation = "DECLINE OR SPECIAL REVIEW"
    
    # Combine top risk factors
    # Combine top risk factors
    all_notable = property_notable + claims_notable + geo_notable + protection_notable
    top_factors = all_notable[:5]  # Top 5 factors
    
    return RiskScores(
        property_risk=round(property_risk, 1),
        claims_risk=round(claims_risk, 1),
        geographic_risk=round(geographic_risk, 1),
        protection_risk=round(protection_risk, 1),
        overall_score=round(overall_score, 1),
        risk_level=risk_level,
        recommendation=recommendation,
        top_factors=top_factors,
        property_factors=property_breakdown,
        claims_factors=claims_breakdown,
        geographic_factors=geo_breakdown,
        protection_factors=protection_breakdown
    )


def process_all_properties(property_df: pd.DataFrame, claims_df: Optional[pd.DataFrame] = None) -> List[Dict]:
    """Process all properties and return risk assessments"""
    results = []
    
    for idx, row in property_df.iterrows():
        risk_scores = calculate_all_risk_scores(row, claims_df)
        
        # Construct composite address
        addr_parts = []
        street = str(row.get('Street Address', ''))
        if street and street.lower() != 'nan':
            addr_parts.append(street)
        else:
            mailing = str(row.get('Mailing Address', ''))
            if mailing and mailing.lower() != 'nan':
                addr_parts.append(mailing)
        
        city = str(row.get('City', ''))
        if city and city.lower() != 'nan':
            addr_parts.append(city)
            
        state = str(row.get('State', ''))
        if state and state.lower() != 'nan':
            addr_parts.append(state)
            
        address_display = ", ".join(addr_parts) if addr_parts else "N/A"

        result = {
            'index': idx + 1,
            'agency_customer_id': row.get('Agency Customer ID', ''),
            'named_insured': row.get('Named Insured', f'Property {idx+1}'),
            'address': address_display,
            'street': street if street.lower() != 'nan' else '',
            'city': city if city.lower() != 'nan' else '',
            'state': state if state.lower() != 'nan' else '',
            'zip': str(row.get('Zip', '')) if str(row.get('Zip', '')).lower() != 'nan' else '',
            'mailing_address': row.get('Mailing Address', 'N/A'),
            'tiv': row.get('TIV (Total Insurable Value)', 17474609),
            'property_risk': risk_scores.property_risk,
            'claims_risk': risk_scores.claims_risk,
            'geographic_risk': risk_scores.geographic_risk,
            'protection_risk': risk_scores.protection_risk,
            'overall_score': risk_scores.overall_score,
            'risk_level': risk_scores.risk_level,
            'recommendation': risk_scores.recommendation,
            'top_factors': risk_scores.top_factors,
            'property_factors': risk_scores.property_factors,
            'claims_factors': risk_scores.claims_factors,
            'geographic_factors': risk_scores.geographic_factors,
            'protection_factors': risk_scores.protection_factors
        }
        results.append(result)
    
    return results


def add_risk_scores_to_df(property_df: pd.DataFrame, claims_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Calculate risk scores for each property and add them as new columns to the DataFrame.
    Returns a copy of the DataFrame with risk score columns added and fallback values populated.
    """
    df = property_df.copy()
    
    # Define defaults for missing columns
    # Detect property name for default selection
    property_name = ""
    if not df.empty:
        for col in ['Named Insured', 'Property Name', 'Insured Name', 'Company']:
            if col in df.columns:
                property_name = str(df.iloc[0][col]).strip()
                break
    
    # Base defaults (Fallback)
    defaults = {
        'TIV (Total Insurable Value)': 17474609,
        'FEMA Flood Zone': 'D',
        'Wildfire Risk Score': 46.89,
        'Earthquake Zone': 'Zone 1',
        'Crime Score': 81,
        'Verified Roof Condition': 'Fair',
        'Construction Type': 'Frame',
        'Year Built': 1989,
        'Sprinklered %': 40,
        'Fire Protection Class': 3,
        'Burglar Alarm Type': 'None',
        'Distance to Fire Station (miles)': 3
    }

    # Conditional Defaults based on Name
    if property_name == "Mudo":
        # High Risk Profile
        defaults.update({
            'Construction Type': 'Frame',          # High Risk
            'Year Built': 1950,                    # Old (High Risk)
            'Verified Roof Condition': 'Poor',     # High Risk
            'TIV (Total Insurable Value)': 2074124,
            'Sprinklered %': 0,                    # High Risk
            'FEMA Flood Zone': 'VE',               # High Risk
            'Wildfire Risk Score': 90.0,           # High Risk
            'Earthquake Zone': 'Zone 4',           # High Risk
            'Crime Score': 90.0,                   # High Risk
            'Fire Protection Class': 9,            # High Risk
            'Burglar Alarm Type': 'None',          # High Risk
            'Distance to Fire Station (miles)': 20 # High Risk
        })
    elif property_name == "Jetwire":
        # Medium Risk Profile
        defaults.update({
            'Construction Type': 'Joisted Masonry', # Medium Risk
            'Year Built': 1990,                     # Medium Age
            'Verified Roof Condition': 'Fair',      # Medium Risk
            'TIV (Total Insurable Value)': 3120088,
            'Sprinklered %': 50,                    # Medium Risk
            'FEMA Flood Zone': 'A',                 # Medium Risk
            'Wildfire Risk Score': 50.0,            # Medium Risk
            'Earthquake Zone': 'Zone 2',            # Medium Risk
            'Crime Score': 50.0,                    # Medium Risk
            'Fire Protection Class': 5,             # Medium Risk
            'Burglar Alarm Type': 'Local',          # Medium Risk
            'Distance to Fire Station (miles)': 8   # Medium Risk
        })
    elif property_name == "Quickbites":
        # Low Risk Profile
        defaults.update({
            'Construction Type': 'Fire Resistive',  # Low Risk
            'Year Built': 2020,                     # New (Low Risk)
            'Verified Roof Condition': 'New',       # Low Risk
            'TIV (Total Insurable Value)': 1896541,
            'Sprinklered %': 100,                   # Low Risk
            'FEMA Flood Zone': 'X',                 # Low Risk
            'Wildfire Risk Score': 10.0,            # Low Risk
            'Earthquake Zone': 'Zone 0',            # Low Risk
            'Crime Score': 10.0,                    # Low Risk
            'Fire Protection Class': 1,             # Low Risk
            'Burglar Alarm Type': 'Central Station',# Low Risk
            'Distance to Fire Station (miles)': 1   # Low Risk
        })

    # Ensure columns exist and fill NaNs
    for col, default_val in defaults.items():
        if col not in df.columns:
            df[col] = default_val
        else:
            # Fill NaN
            df[col] = df[col].fillna(default_val)
            # Fill empty strings if column is object/string type
            if df[col].dtype == object:
                 df.loc[df[col].astype(str).str.strip() == '', col] = default_val

    # Initialize new columns
    df['Property_Risk_Score'] = 0.0
    df['Claims_Risk_Score'] = 0.0
    df['Geographic_Risk_Score'] = 0.0
    df['Protection_Risk_Score'] = 0.0
    df['Overall_Risk_Score'] = 0.0
    df['Risk_Level'] = ''
    df['Recommendation'] = ''
    df['Top_Risk_Factors'] = ''
    
    for idx, row in df.iterrows():
        risk_scores = calculate_all_risk_scores(row, claims_df)
        
        df.at[idx, 'Property_Risk_Score'] = risk_scores.property_risk
        df.at[idx, 'Claims_Risk_Score'] = risk_scores.claims_risk
        df.at[idx, 'Geographic_Risk_Score'] = risk_scores.geographic_risk
        df.at[idx, 'Protection_Risk_Score'] = risk_scores.protection_risk
        df.at[idx, 'Overall_Risk_Score'] = risk_scores.overall_score
        df.at[idx, 'Risk_Level'] = risk_scores.risk_level
        df.at[idx, 'Recommendation'] = risk_scores.recommendation
        df.at[idx, 'Top_Risk_Factors'] = ' | '.join(risk_scores.top_factors)
    
    return df


def generate_summary_stats(results: List[Dict]) -> Dict:
    """Generate aggregate statistics from results"""
    total = len(results)
    low_risk = sum(1 for r in results if r['risk_level'] == 'LOW')
    medium_risk = sum(1 for r in results if r['risk_level'] == 'MEDIUM')
    high_risk = sum(1 for r in results if r['risk_level'] == 'HIGH')
    very_high_risk = sum(1 for r in results if r['risk_level'] == 'VERY HIGH')
    total_tiv = sum(r['tiv'] for r in results)
    avg_score = sum(r['overall_score'] for r in results) / total if total > 0 else 0
    
    return {
        'total_properties': total,
        'low_risk_count': low_risk,
        'medium_risk_count': medium_risk,
        'high_risk_count': high_risk,
        'very_high_risk_count': very_high_risk,
        'total_tiv': total_tiv,
        'average_score': round(avg_score, 1)
    }




def generate_analysis_summary(result: Dict, llm=None) -> str:
    """Generate a contextual summary paragraph with recommendations based on risk analysis"""
    
    risk_level = result['risk_level']
    overall_score = result['overall_score']
    property_risk = result['property_risk']
    claims_risk = result['claims_risk']
    geographic_risk = result['geographic_risk']
    protection_risk = result['protection_risk']
    top_factors = result.get('top_factors', [])
    
    # Identify primary risk drivers
    risk_drivers = []
    if property_risk >= 60:
        risk_drivers.append("property characteristics")
    if claims_risk >= 60:
        risk_drivers.append("claims history")
    if geographic_risk >= 60:
        risk_drivers.append("geographic location")
    if protection_risk >= 60:
        risk_drivers.append("protection systems")
    
    risk_drivers_str = ", ".join(risk_drivers) if risk_drivers else "balanced across categories"
    top_factors_str = ", ".join(top_factors) if top_factors else "None"
    
    # If LLM is provided, use it to generate the summary
    if llm:
        try:
            from langchain_core.messages import HumanMessage
            
            prompt = ANALYSIS_SUMMARY_PROMPT.format(
                risk_level=risk_level,
                overall_score=int(overall_score),
                risk_drivers=risk_drivers_str,
                top_factors=top_factors_str,
                property_risk=int(property_risk),
                claims_risk=int(claims_risk),
                geographic_risk=int(geographic_risk),
                protection_risk=int(protection_risk)
            )
            
            response = llm.invoke([HumanMessage(content=prompt)])
            return response.content.strip()
            
        except Exception as e:
            print(f"Error generating LLM summary: {e}. Falling back to hardcoded.")
            # Fall through to hardcoded logic
            pass
            
    # Fallback to hardcoded logic
    # Base summary based on risk level
    if risk_level == "VERY HIGH":
        base_summary = f"This property presents a **very high claim likelihood** with an overall score of **{int(overall_score)}%**. "
        action = "This property requires **immediate senior underwriter review** or should be considered for **declination**. "
    elif risk_level == "HIGH":
        base_summary = f"This property shows a **high claim likelihood** with an overall score of **{int(overall_score)}%**. "
        action = "This property should be **referred to a senior underwriter** for detailed evaluation before binding. "
    elif risk_level == "MEDIUM":
        base_summary = f"This property has a **moderate claim likelihood** with an overall score of **{int(overall_score)}%**. "
        action = "This property can proceed through **standard underwriting review** with careful attention to the identified risk factors. "
    else:  # LOW
        base_summary = f"This property demonstrates a **low claim likelihood** with an overall score of **{int(overall_score)}%**. "
        action = "This property is **eligible for auto-bind** subject to standard policy terms and conditions. "
    
    
    if risk_drivers:
        drivers_text = f"The primary risk drivers are **{', '.join(risk_drivers)}**. "
    else:
        drivers_text = "The risk profile is relatively balanced across all categories. "
    
    
    
    # Generate specific recommendations based on both risk scores and top factors
    recommendations = []
    
    # First, analyze top factors directly for the most critical issues
    for factor in top_factors[:3]:  # Focus on top 3 factors
        factor_lower = factor.lower()
        
        # Construction-related
        if 'construction type' in factor_lower and 'frame' in factor_lower:
            recommendations.append("**Require** proof of upgraded fire protection systems as a condition of binding")
        
        # Sprinkler-related
        if 'sprinkler' in factor_lower:
            recommendations.append("**Mandate** sprinkler system installation or apply **premium surcharge** for inadequate protection")
        
        # Claims-related
        if 'claim count' in factor_lower or 'high claim' in factor_lower:
            recommendations.append("**Impose** minimum deductible of $5,000+ to discourage claim frequency")
        if 'loss amount' in factor_lower:
            recommendations.append("**Apply** sub-limits on high-severity perils and consider **co-insurance clause**")
        
        # Fire-related
        if 'fire' in factor_lower and 'loss' in factor_lower:
            recommendations.append("**Obtain** current fire protection system inspection certificate before binding")
        
        # Geographic risks
        if 'wildfire' in factor_lower:
            recommendations.append("**Require** defensible space certification and consider **wildfire exclusion** if non-compliant")
        if 'flood' in factor_lower:
            recommendations.append("**Exclude** flood coverage and advise separate NFIP or private flood policy")
        if 'crime' in factor_lower:
            recommendations.append("**Require** central station monitored security system as binding condition")
        
        # Building age
        if 'age' in factor_lower or 'year built' in factor_lower:
            recommendations.append("**Order** professional building inspection to assess structural integrity and systems")
        
        # Roof condition
        if 'roof' in factor_lower:
            recommendations.append("**Require** roof certification or **exclude** wind/hail coverage until roof is replaced")
    
    # Then add category-specific recommendations based on risk scores
    # Property-specific recommendations
    if property_risk >= 60:
        property_factors = result.get('property_factors', [])
        if any('Construction' in f and ('Frame' in f or 'Wood' in f) for f in property_factors):
            if not any('fire protection' in r.lower() for r in recommendations):
                recommendations.append("**Require** proof of upgraded fire protection systems as a condition of binding")
        if any('Age' in f or 'Year Built' in f for f in property_factors):
            if not any('inspection' in r.lower() for r in recommendations):
                recommendations.append("**Order** professional building inspection to assess structural integrity and systems")
        if any('Roof' in f for f in property_factors):
            if not any('roof' in r.lower() for r in recommendations):
                recommendations.append("**Require** roof certification or **exclude** wind/hail coverage until roof is replaced")
        if any('Sprinkler' in f for f in property_factors):
            if not any('sprinkler' in r.lower() for r in recommendations):
                recommendations.append("**Mandate** sprinkler system installation or apply **premium surcharge** for inadequate protection")
    
    # Claims history recommendations
    if claims_risk >= 60:
        claims_factors = result.get('claims_factors', [])
        if any('Count' in f for f in claims_factors):
            if not any('deductible' in r.lower() for r in recommendations):
                recommendations.append("**Impose** minimum deductible of $5,000+ to discourage claim frequency")
        if any('Amount' in f or 'Total' in f for f in claims_factors):
            if not any('sub-limit' in r.lower() for r in recommendations):
                recommendations.append("**Apply** sub-limits on high-severity perils and consider **co-insurance clause**")
        if any('Fire' in str(f) for f in claims_factors):
            if not any('fire protection systems' in r.lower() for r in recommendations):
                recommendations.append("**Obtain** current fire protection system inspection certificate before binding")
    
    # Geographic recommendations
    if geographic_risk >= 60:
        geo_factors = result.get('geographic_factors', [])
        if any('Wildfire' in f for f in geo_factors):
            if not any('wildfire' in r.lower() for r in recommendations):
                recommendations.append("**Require** defensible space certification and consider **wildfire exclusion** if non-compliant")
        if any('Flood' in f for f in geo_factors):
            if not any('flood' in r.lower() for r in recommendations):
                recommendations.append("**Exclude** flood coverage and advise separate NFIP or private flood policy")
        if any('Earthquake' in f for f in geo_factors):
            if not any('earthquake' in r.lower() for r in recommendations):
                recommendations.append("**Offer** earthquake coverage as **optional endorsement** with separate premium")
        if any('Crime' in f for f in geo_factors):
            if not any('security' in r.lower() for r in recommendations):
                recommendations.append("**Require** central station monitored security system as binding condition")
    
    # Protection recommendations
    if protection_risk >= 60:
        protection_factors = result.get('protection_factors', [])
        if any('Fire Protection Class' in f for f in protection_factors):
            if not any('limited fire protection' in r.lower() for r in recommendations):
                recommendations.append("**Apply** premium surcharge due to poor fire protection class; consider **coverage restrictions**")
        if any('Burglar Alarm' in f for f in protection_factors):
            if not any('burglar alarm' in r.lower() and 'security' not in r.lower() for r in recommendations):
                recommendations.append("**Require** installation of central station burglar alarm before policy issuance")
        if any('Fire Station' in f for f in protection_factors):
            if not any('premium' in r.lower() for r in recommendations):
                recommendations.append("**Apply** distance-to-fire-station premium surcharge per rating guidelines")
    
    # If no specific recommendations, provide general guidance
    if not recommendations:
        if risk_level in ["LOW", "MEDIUM"]:
            recommendations.append("**Approve** with standard policy terms and competitive pricing")
            recommendations.append("**Consider** for preferred risk tier with enhanced coverage options")
        else:
            recommendations.append("**Escalate** to senior underwriter for comprehensive risk assessment")
            recommendations.append("**Consider** declination or require substantial risk improvements before binding")
    
    # Format recommendations as a bulleted list
    if recommendations:
        rec_text = "\n\n**Suggested Actions:**\n" + "\n".join([f"- {rec}" for rec in recommendations[:5]])  # Limit to top 5
    else:
        rec_text = ""
    
    # Combine all parts
    summary = base_summary + drivers_text + action + rec_text
    
    return summary



def format_property_summary(result: Dict, llm=None) -> str:
    """Format a single property's claim likelihood assessment as markdown"""
    factors_text = "\n".join([f"- {f}" for f in result['top_factors']]) if result['top_factors'] else "- No major claim likelihood factors"
    
    # Claim likelihood level colors/emoji
    score = result['overall_score']
    if result['risk_level'] == "VERY HIGH":
        color_block = "🟥🟥🟥"
        score_color = "red"
        emoji = "🔴"
    elif result['risk_level'] == "HIGH":
        color_block = "🟧🟧🟧"
        score_color = "orange"
        emoji = "🟠"
    elif result['risk_level'] == "MEDIUM":
        color_block = "🟨🟨🟨"
        score_color = "gold"
        emoji = "🟡"
    else:
        color_block = "🟩🟩🟩"
        score_color = "green"
        emoji = "🟢"
    
    # Components for address
    street = result.get('street', '')
    city = result.get('city', '')
    state = result.get('state', '')
    zip_code = result.get('zip', '')
    
    # Build address string intelligently
    addr_lines = []
    if street:
        addr_lines.append(f"**Address:** {street}")
    
    location_parts = []
    if city: location_parts.append(f"**City:** {city}")
    if state: location_parts.append(f"**State:** {state}")
    if zip_code: location_parts.append(f"**Zip:** {zip_code}")
    
    if location_parts:
        addr_lines.append(", ".join(location_parts))
        
    address_section = "\n".join(addr_lines) if addr_lines else "**Address:** Not available in data"


    def get_severity_bar(score):
        filled = int(score / 20)
        # Ensure max 5
        filled = min(filled, 5)
        # Ensure min 1 if score > 0 effectively, but int(1/20) is 0. 
        # Actually logic says 0-19: 0 dots, 20-39: 1 dot... 
        # Better: 1 dot per 20%. 
        
        if score < 40:
             color_char = '🟩'
        elif score < 70:
             color_char = '🟨'
        else:
             color_char = '🟥'
             
        return f"{color_char * filled}{'⬜' * (5 - filled)}"

    # Generate contextual summary and recommendations
    summary_paragraph = generate_analysis_summary(result, llm)
    
    return f"""
### 🏢 {result['named_insured']}

**📍 Property Details**
{address_section}

**💰 TIV:** ${result['tiv']:,.2f}

**🔍 Risk Breakdown**

| **Category** | **Severity** | **Contributing Factors** |
|----------|----------|----------------------|
| **Property** | {get_severity_bar(result['property_risk'])} | {'<br>'.join(result.get('property_factors', []))} |
| **Claims History** | {get_severity_bar(result['claims_risk'])} | {'<br>'.join(result.get('claims_factors', []))} |
| **Geographic** | {get_severity_bar(result['geographic_risk'])} | {'<br>'.join(result.get('geographic_factors', []))} |
| **Protection** | {get_severity_bar(result['protection_risk'])} | {'<br>'.join(result.get('protection_factors', []))} |

**📊 Overall Claim Likelihood:**
{color_block} {int(score)}% - {result['risk_level']} Likelihood


**⚠️ Top Factors Contributing to Score:**
{factors_text}

---
#### 📋 Recommendation: {result['recommendation']}

#### 📝 Analysis Summary & Recommendations

{summary_paragraph}
"""




def format_aggregate_summary(stats: Dict, named_insured: str = None) -> str:
    """Format aggregate summary as markdown"""
    header = f"#### 📊 PORTFOLIO CLAIM LIKELIHOOD SUMMARY"
    if named_insured:
        header += f"\n\n**Client Name:** {named_insured}"
    
    return f"""
{header}

**Total Properties Analyzed:** {stats['total_properties']}

**Claim Likelihood Distribution**

| Claim Likelihood Level | Count | Percentage |
|------------------------|-------|------------|
| 🟢 Low (Auto-Bind) | {stats['low_risk_count']} | {stats['low_risk_count']/stats['total_properties']*100:.1f}% |
| 🟡 Medium (Standard) | {stats['medium_risk_count']} | {stats['medium_risk_count']/stats['total_properties']*100:.1f}% |
| 🟠 High (Refer) | {stats['high_risk_count']} | {stats['high_risk_count']/stats['total_properties']*100:.1f}% |
| 🔴 Very High (Decline) | {stats['very_high_risk_count']} | {stats['very_high_risk_count']/stats['total_properties']*100:.1f}% |

**Financial Summary**

| Metric | Value |
|--------|-------|
| **Total Insured Value (TIV)** | ${stats['total_tiv']:,.2f} |
| **Average Claim Likelihood** | {stats['average_score']:.1f}% |
"""


# =============================================================================
# GENERAL DATA QUERY FUNCTION
# =============================================================================

def get_dataframe_schema(df: pd.DataFrame) -> str:
    """Generate a schema description of the DataFrame for LLM context"""
    schema_lines = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sample_values = df[col].dropna().head(3).tolist()
        sample_str = ", ".join([str(v)[:30] for v in sample_values])
        schema_lines.append(f"- {col} ({dtype}): e.g., {sample_str}")
    return "\n".join(schema_lines)


def get_sample_data(df: pd.DataFrame, n_rows: int = 3) -> str:
    """Get sample rows from DataFrame as string"""
    return df.head(n_rows).to_string()


def execute_pandas_query(df: pd.DataFrame, code: str) -> Tuple[bool, any]:
    """
    Safely execute pandas query code.
    Returns (success, result) tuple.
    """
    import numpy as np
    
    # Clean up the code
    code = code.strip()
    
    # Remove markdown code blocks if present
    if code.startswith("```"):
        lines = code.split("\n")
        code = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    
    # Security check - block dangerous operations
    dangerous_patterns = [
        'import ', 'exec(', 'eval(', 'open(', 'file(', 
        '__', 'os.', 'sys.', 'subprocess', 'shutil',
        'read(', 'write(', 'delete', 'remove', 'system'
    ]
    
    code_lower = code.lower()
    for pattern in dangerous_patterns:
        if pattern.lower() in code_lower:
            return False, f"Security error: '{pattern}' is not allowed"
    
    # Create restricted namespace
    safe_namespace = {
        'df': df,
        'pd': pd,
        'np': np,
        'str': str,
        'int': int,
        'float': float,
        'len': len,
        'list': list,
        'dict': dict,
        'set': set,
        'min': min,
        'max': max,
        'sum': sum,
    }
    
    try:
        # Execute the code
        exec(code, {"__builtins__": {}}, safe_namespace)
        result = safe_namespace.get('result', None)
        
        if result is None:
            return False, "No result variable found in code"
        
        # Convert result to serializable format
        if isinstance(result, pd.DataFrame):
            if len(result) > 10:
                result = result.head(10).to_dict('records')
            else:
                result = result.to_dict('records')
        elif isinstance(result, pd.Series):
            result = result.to_dict()
        elif hasattr(result, 'item'):  # numpy scalar
            result = result.item()
            
        return True, result
        
    except Exception as e:
        return False, f"Execution error: {str(e)}"


def general_data_query(df: pd.DataFrame, user_query: str, llm) -> str:
    """
    Handle general questions about the uploaded data using LLM.
    
    Flow:
    1. Generate pandas code to answer the query
    2. Execute the code safely
    3. Format and return the response
    
    Args:
        df: The DataFrame containing uploaded data
        user_query: The user's natural language question
        llm: LangChain LLM instance
        
    Returns:
        Natural language response answering the query
    """
    from langchain_core.messages import HumanMessage
    
    # Step 1: Generate pandas code
    schema = get_dataframe_schema(df)
    sample_data = get_sample_data(df, 3)
    
    # Add explicit column list for clarity
    columns_list = "\\nAvailable columns: " + ", ".join(df.columns.tolist())
    
    planning_prompt = DATA_QUERY_PLANNING_PROMPT.format(
        user_query=user_query,
        schema=schema + columns_list,
        sample_data=sample_data,
        current_year=2025
    )
    
    planning_response = llm.invoke([HumanMessage(content=planning_prompt)])
    pandas_code = planning_response.content.strip()
    
    # Step 2: Execute the query
    success, result = execute_pandas_query(df, pandas_code)
    
    if not success:
        # If execution failed, return error message
        return f"❌ I couldn't process that query. Error: {result}\\n\\nTry rephrasing your question or use specific commands like 'list' or 'details [property name]'."
    
    # Step 3: Generate natural language response
    response_prompt = DATA_QUERY_RESPONSE_PROMPT.format(
        user_query=user_query,
        result=str(result)
    )
    
    final_response = llm.invoke([HumanMessage(content=response_prompt)])
    
    return final_response.content


async def general_data_query_streaming(df: pd.DataFrame, user_query: str, llm, msg) -> None:
    """
    Handle general questions about the uploaded data using LLM with streaming.
    
    Flow:
    1. Generate pandas code to answer the query
    2. Execute the code safely
    3. Stream the formatted response token-by-token
    
    Args:
        df: The DataFrame containing uploaded data
        user_query: The user's natural language question
        llm: LangChain LLM instance
        msg: Chainlit message object to stream to
    """
    from langchain_core.messages import HumanMessage
    
    # Step 1: Generate pandas code
    schema = get_dataframe_schema(df)
    sample_data = get_sample_data(df, 3)
    
    # Add explicit column list for clarity
    columns_list = "\\nAvailable columns: " + ", ".join(df.columns.tolist())
    
    planning_prompt = DATA_QUERY_PLANNING_PROMPT.format(
        user_query=user_query,
        schema=schema + columns_list,
        sample_data=sample_data,
        current_year=2025
    )
    
    planning_response = llm.invoke([HumanMessage(content=planning_prompt)])
    pandas_code = planning_response.content.strip()
    
    # Step 2: Execute the query
    success, result = execute_pandas_query(df, pandas_code)
    
    if not success:
        # If execution failed, send error message
        msg.content = f"❌ I couldn't process that query. Error: {result}\\n\\nTry rephrasing your question or use specific commands like 'list' or 'details [property name]'."
        await msg.update()
        return
    
    # Step 3: Stream natural language response
    response_prompt = DATA_QUERY_RESPONSE_PROMPT.format(
        user_query=user_query,
        result=str(result)
    )
    
    # Stream the response
    async for chunk in llm.astream([HumanMessage(content=response_prompt)]):
        if chunk.content:
            await msg.stream_token(chunk.content)
    
    await msg.update()
