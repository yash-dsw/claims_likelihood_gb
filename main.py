"""
Main orchestrator for PDF-based Claims Likelihood Analysis
Extracts data from PDF attachments, performs risk analysis, and generates reports
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Tuple
import traceback

from extract_pdf_fields import extract_pdf_form_fields
from utils import (
    calculate_all_risk_scores,
    add_risk_scores_to_df,
    generate_analysis_summary
)
from pdf_gen import generate_claims_likelihood_report


class ClaimsAnalysisOrchestrator:
    """Orchestrates the complete claims likelihood analysis workflow"""
    
    def __init__(self, output_dir: str = "./analysis_output"):
        """
        Initialize the orchestrator
        
        Args:
            output_dir: Directory to store output files
        """
        self.output_dir = output_dir
        self.reports_dir = os.path.join(output_dir, "reports")
        self.data_dir = os.path.join(output_dir, "data")
        
        # Create output directories
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
    
    def extract_data_from_pdf(self, pdf_path: str) -> Tuple[bool, Dict, str]:
        """
        Extract form fields from PDF
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Tuple of (success, extracted_data_dict, error_message)
        """
        try:
            print(f"[1/4] Extracting data from PDF: {pdf_path}")
            
            if not os.path.exists(pdf_path):
                return False, {}, f"PDF file not found: {pdf_path}"
            
            extracted_data = extract_pdf_form_fields(pdf_path)
            
            if not extracted_data or all(not v for v in extracted_data.values()):
                return False, {}, "No data could be extracted from PDF"
            
            # Count populated fields
            populated_count = len([v for v in extracted_data.values() if v])
            print(f"   ✓ Successfully extracted {populated_count} fields")
            
            return True, extracted_data, ""
            
        except Exception as e:
            error_msg = f"PDF extraction failed: {str(e)}\n{traceback.format_exc()}"
            print(f"   ✗ {error_msg}")
            return False, {}, error_msg
    
    def prepare_dataframes(self, extracted_data: Dict) -> Tuple[bool, pd.DataFrame, pd.DataFrame, str]:
        """
        Convert extracted data to DataFrames
        
        Args:
            extracted_data: Dictionary of extracted PDF fields
            
        Returns:
            Tuple of (success, property_df, claims_df, error_message)
        """
        try:
            print(f"[2/4] Preparing data for analysis")
            
            # Create a copy for DataFrame conversion
            df_data = extracted_data.copy()
            
            # Extract Loss Types from Loss History if available
            if 'Loss History' in extracted_data and isinstance(extracted_data['Loss History'], list):
                loss_history = extracted_data['Loss History']
                types = set(entry.get('Type', '') for entry in loss_history if entry.get('Type'))
                if types:
                    df_data['Loss History - Type'] = ", ".join(types)
            
            # Convert Loss History array to JSON string for DataFrame storage
            if 'Loss History' in df_data and isinstance(df_data['Loss History'], list):
                df_data['Loss History'] = json.dumps(df_data['Loss History'])
            
            # Create property DataFrame with single row
            property_df = pd.DataFrame([df_data])
            
            # Create claims DataFrame from Loss History if present
            claims_df = pd.DataFrame()
            if 'Loss History' in extracted_data and extracted_data['Loss History']:
                loss_history = extracted_data['Loss History']
                if isinstance(loss_history, list) and len(loss_history) > 0:
                    claims_data = []
                    for loss_entry in loss_history:
                        claim_record = loss_entry.copy()
                        claim_record['Property'] = extracted_data.get('Named Insured', '')
                        claim_record['Agency Customer ID'] = extracted_data.get('Agency Customer ID', '')
                        claim_record['Street Address'] = extracted_data.get('Street Address', '')
                        claims_data.append(claim_record)
                    
                    if claims_data:
                        claims_df = pd.DataFrame(claims_data)
            
            print(f"   ✓ Property data prepared: {len(property_df)} property")
            print(f"   ✓ Claims data prepared: {len(claims_df)} claims")
            
            return True, property_df, claims_df, ""
            
        except Exception as e:
            error_msg = f"Data preparation failed: {str(e)}\n{traceback.format_exc()}"
            print(f"   ✗ {error_msg}")
            return False, pd.DataFrame(), pd.DataFrame(), error_msg
    
    def perform_risk_analysis(self, property_df: pd.DataFrame, claims_df: pd.DataFrame) -> Tuple[bool, pd.DataFrame, Dict, str]:
        """
        Perform risk scoring and analysis
        
        Args:
            property_df: Property data DataFrame
            claims_df: Claims history DataFrame
            
        Returns:
            Tuple of (success, scored_df, analysis_summary, error_message)
        """
        try:
            print(f"[3/4] Performing risk analysis")
            
            # Add risk scores to DataFrame
            scored_df = add_risk_scores_to_df(property_df, claims_df if not claims_df.empty else None)
            
            # Get the property row with scores
            if len(scored_df) == 0:
                return False, pd.DataFrame(), {}, "No properties found for analysis"
            
            property_row = scored_df.iloc[0]
            
            # Calculate comprehensive risk scores
            risk_scores = calculate_all_risk_scores(property_row, claims_df if not claims_df.empty else None)
            
            # Build analysis summary dictionary
            analysis_summary = {
                'named_insured': property_row.get('Named Insured', 'Unknown'),
                'overall_score': risk_scores.overall_score,
                'risk_level': risk_scores.risk_level,
                'recommendation': risk_scores.recommendation,
                'property_risk': risk_scores.property_risk,
                'claims_risk': risk_scores.claims_risk,
                'geographic_risk': risk_scores.geographic_risk,
                'protection_risk': risk_scores.protection_risk,
                'top_factors': risk_scores.top_factors,
                'property_factors': risk_scores.property_factors,
                'claims_factors': risk_scores.claims_factors,
                'geographic_factors': risk_scores.geographic_factors,
                'protection_factors': risk_scores.protection_factors,
            }
            
            print(f"   ✓ Risk analysis complete")
            print(f"   ✓ Overall Risk Score: {risk_scores.overall_score:.1f}% ({risk_scores.risk_level})")
            print(f"   ✓ Recommendation: {risk_scores.recommendation}")
            
            return True, scored_df, analysis_summary, ""
            
        except Exception as e:
            error_msg = f"Risk analysis failed: {str(e)}\n{traceback.format_exc()}"
            print(f"   ✗ {error_msg}")
            return False, pd.DataFrame(), {}, error_msg
    
    def generate_pdf_report(self, property_df: pd.DataFrame, claims_df: pd.DataFrame, 
                          scored_df: pd.DataFrame, client_name: str, input_pdf_name: str = None) -> Tuple[bool, str, str]:
        """
        Generate PDF report
        
        Args:
            property_df: Original property data
            claims_df: Claims history data
            scored_df: Property data with risk scores
            client_name: Client name for filename
            input_pdf_name: Optional input PDF filename to base output name on
            
        Returns:
            Tuple of (success, pdf_path, error_message)
        """
        try:
            print(f"[4/4] Generating PDF report")
            
            # Optional: Set logo path if available
            logo_path = "./public/golden_bear.png" if os.path.exists("./public/golden_bear.png") else None
            
            # Generate PDF report
            pdf_path = generate_claims_likelihood_report(
                input_df=property_df,
                claims_df=claims_df if not claims_df.empty else pd.DataFrame(),
                output_df=scored_df,
                logo_path=logo_path,
                input_pdf_name=input_pdf_name
            )
            
            # Move PDF to reports directory if not already there
            if not pdf_path.startswith(self.reports_dir):
                filename = os.path.basename(pdf_path)
                new_path = os.path.join(self.reports_dir, filename)
                
                # Copy file if original still exists
                if os.path.exists(pdf_path):
                    import shutil
                    shutil.move(pdf_path, new_path)
                    pdf_path = new_path
            
            print(f"   ✓ PDF report generated: {pdf_path}")
            
            return True, pdf_path, ""
            
        except Exception as e:
            error_msg = f"PDF generation failed: {str(e)}\n{traceback.format_exc()}"
            print(f"   ✗ {error_msg}")
            return False, "", error_msg
    
    def save_intermediate_data(self, scored_df: pd.DataFrame, analysis_summary: Dict, 
                              client_name: str) -> None:
        """
        Save intermediate data files (CSV and JSON)
        
        Args:
            scored_df: DataFrame with risk scores
            analysis_summary: Analysis summary dictionary
            client_name: Client name for filenames
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = client_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            
            # Save CSV
            csv_path = os.path.join(self.data_dir, f"analysis_{safe_name}_{timestamp}.csv")
            scored_df.to_csv(csv_path, index=False)
            print(f"   ✓ Data saved to CSV: {csv_path}")
            
            # Save JSON summary
            json_path = os.path.join(self.data_dir, f"summary_{safe_name}_{timestamp}.json")
            with open(json_path, 'w') as f:
                json.dump(analysis_summary, f, indent=2)
            print(f"   ✓ Summary saved to JSON: {json_path}")
            
        except Exception as e:
            print(f"   ⚠ Warning: Could not save intermediate data: {str(e)}")


def analyze_pdf_attachment(pdf_path: str, output_dir: str = "./analysis_output") -> Dict:
    """
    Main entry point for PDF analysis workflow
    Extracts data, performs risk analysis, and generates report
    
    Args:
        pdf_path: Path to the PDF attachment to analyze
        output_dir: Directory for output files
        
    Returns:
        Dictionary with results:
        {
            'success': bool,
            'pdf_report_path': str,
            'analysis_summary': dict,
            'error_message': str
        }
    """
    print("\n" + "="*70)
    print("CLAIMS LIKELIHOOD ANALYSIS - STARTING")
    print("="*70)
    
    orchestrator = ClaimsAnalysisOrchestrator(output_dir)
    
    # Step 1: Extract data from PDF
    success, extracted_data, error = orchestrator.extract_data_from_pdf(pdf_path)
    if not success:
        return {
            'success': False,
            'pdf_report_path': '',
            'analysis_summary': {},
            'error_message': error
        }
    
    # Step 2: Prepare DataFrames
    success, property_df, claims_df, error = orchestrator.prepare_dataframes(extracted_data)
    if not success:
        return {
            'success': False,
            'pdf_report_path': '',
            'analysis_summary': {},
            'error_message': error
        }
    
    # Step 3: Perform risk analysis
    success, scored_df, analysis_summary, error = orchestrator.perform_risk_analysis(property_df, claims_df)
    if not success:
        return {
            'success': False,
            'pdf_report_path': '',
            'analysis_summary': {},
            'error_message': error
        }
    
    # Get client name for filenames
    client_name = analysis_summary.get('named_insured', 'Property')
    
    # Save intermediate data
    orchestrator.save_intermediate_data(scored_df, analysis_summary, client_name)
    
    # Step 4: Generate PDF report
    success, pdf_path, error = orchestrator.generate_pdf_report(property_df, claims_df, scored_df, client_name)
    if not success:
        return {
            'success': False,
            'pdf_report_path': '',
            'analysis_summary': analysis_summary,  # Return analysis even if PDF fails
            'error_message': error
        }
    
    print("\n" + "="*70)
    print("CLAIMS LIKELIHOOD ANALYSIS - COMPLETED SUCCESSFULLY")
    print("="*70)
    print(f"\nReport saved to: {pdf_path}\n")
    
    return {
        'success': True,
        'pdf_report_path': pdf_path,
        'analysis_summary': analysis_summary,
        'error_message': ''
    }


# For testing/standalone execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python main.py <path_to_pdf>")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    result = analyze_pdf_attachment(pdf_path)
    
    if result['success']:
        print(f"\n✓ Analysis completed successfully!")
        print(f"PDF Report: {result['pdf_report_path']}")
        print(f"\nRisk Summary:")
        print(f"  - Overall Score: {result['analysis_summary']['overall_score']:.1f}%")
        print(f"  - Risk Level: {result['analysis_summary']['risk_level']}")
        print(f"  - Recommendation: {result['analysis_summary']['recommendation']}")
    else:
        print(f"\n✗ Analysis failed: {result['error_message']}")
        sys.exit(1)
