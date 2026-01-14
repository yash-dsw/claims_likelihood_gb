"""
PDF Report Generator for Claims Likelihood Analysis
Generates professional reports for commercial property underwriting
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
from datetime import datetime
import os
import pandas as pd


class ClaimsLikelihoodReportGenerator:
    """Generates claims likelihood analysis PDF reports"""
    
    def __init__(self, input_df: pd.DataFrame, claims_df: pd.DataFrame, 
                 output_df: pd.DataFrame, logo_path: str = None):
        """
        Initialize the report generator
        
        Args:
            input_df: Original property data with building details
            claims_df: Claims history data (can be None or empty)
            output_df: Analyzed data with risk scores
            logo_path: Optional path to company logo
        """
        self.input_df = input_df
        self.claims_df = claims_df if claims_df is not None and len(claims_df) > 0 else None
        self.output_df = output_df
        self.logo_path = logo_path
        
        # Extract property details (assuming single property)
        if len(input_df) > 0:
            self.property_row = input_df.iloc[0]
        else:
            raise ValueError("Input DataFrame is empty")
            
        if len(output_df) > 0:
            self.output_row = output_df.iloc[0]
        else:
            raise ValueError("Output DataFrame is empty")
    
    def _format_currency(self, value):
        """Format value as currency"""
        try:
            val = float(value)
            return f"${val:,.2f}"
        except:
            return "N/A"
    
    def _format_percentage(self, value):
        """Format value as percentage"""
        try:
            val = float(value)
            return f"{val:.1f}%"
        except:
            return "N/A"
    
    def _safe_get(self, row, column, default="N/A"):
        """Safely get value from row"""
        try:
            val = row.get(column, default)
            if pd.isna(val) or str(val).lower() == 'nan':
                return default
            return str(val)
        except:
            return default
    
    def _wrap_text(self, text, max_chars):
        """Wrap text to fit within character limit"""
        words = text.split()
        lines = []
        current_line = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) + 1 <= max_chars:
                current_line.append(word)
                current_length += len(word) + 1
            else:
                if current_line:
                    lines.append(' '.join(current_line))
                current_line = [word]
                current_length = len(word)
        
        if current_line:
            lines.append(' '.join(current_line))
        
        return lines
    
    def get_filename(self):
        """Generate filename for the report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        client_name = self._safe_get(self.property_row, 'Named Insured', 'Property')
        safe_name = client_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        
        save_dir = "./reports"
        os.makedirs(save_dir, exist_ok=True)
        output_path = os.path.join(save_dir, f"Underwriting_Report_{safe_name}_{timestamp}.pdf")
        return output_path
    
    def _draw_header(self, c, width, height):
        """Draw report header"""
        # Logo if available - positioned at top right corner
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                # Position logo in top right corner with padding
                logo_x = width - 250  # 50pt from right edge
                # logo_y = height - 60  # 60pt from top
                logo_y = height - 120  # Aligned with title
                c.drawImage(self.logo_path, logo_x, logo_y, 
                           width=200, height=100, 
                           preserveAspectRatio=True, mask='auto')
            except Exception as e:
                print(f"Warning: Could not load logo: {e}")
        
        # Title
        c.setFont("Helvetica-Bold", 16)
        # c.drawString(50, height - 70, "UNDERWRITING REPORT FOR CLAIMS LIKELIHOOD")
        c.drawString(50, height - 70, "UNDERWRITING REPORT FOR")
        c.drawString(50, height - 90, "CLAIMS LIKELIHOOD")

        c.setFont("Helvetica", 11)
        c.drawRightString(width - 50, height - 100, f"{datetime.now().strftime('%B %d, %Y')}")
        
        return height - 130
    
    def _draw_section_header(self, c, y_pos, title, width):
        """Draw a section header"""
        c.setFillColor(colors.HexColor('#ffcd69'))
        c.rect(50, y_pos - 2, width - 100, 22, fill=True, stroke=False)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(60, y_pos + 5, title)
        c.setFillColor(colors.black)
        return y_pos - 18
    
    def _draw_key_value_section(self, c, y_pos, data_pairs):
        """Draw key-value pairs"""
        c.setFont("Helvetica", 11)
        for label, value in data_pairs:
            c.setFont("Helvetica-Bold", 9)
            c.drawString(60, y_pos, str(label) + ":")
            c.setFont("Helvetica", 9)
            c.drawString(250, y_pos, str(value))
            y_pos -= 16
        return y_pos - 10
    
    def _extract_client_details(self):
        """Extract client and property details"""
        # Use flexible column finding for better ACORD form compatibility
        df = self.input_df
        
        client_name_col = self._find_column(df, ['Named Insured', 'Insured', 'Applicant Name', 'Policyholder'])
        address_col = self._find_column(df, ['Street Address', 'Mailing Address', 'Property Address', 'Address'])
        city_col = self._find_column(df, ['City/State', 'City', 'City State', 'Location'])
        business_col = self._find_column(df, ['Business Description', 'Business Type', 'Type of Business', 'Description', 'Subject of Insurance'])
        naics_col = self._find_column(df, ['NAICS Code', 'NAICS', 'Industry Code'])
        year_col = self._find_column(df, ['Year Built', 'Construction Year', 'Year'])
        tiv_col = self._find_column(df, ['TIV (Total Insurable Value)', 'TIV', 'Total Insurable Value', 'Limit of Insurance', 'Limit'])
        years_biz_col = self._find_column(df, ['Years in Business', 'Years Operating', 'Business Years'])
        
        return {
            'client_name': self._safe_get(self.property_row, client_name_col) if client_name_col else 'N/A',
            'property_address': self._safe_get(self.property_row, address_col) if address_col else 'N/A',
            'city_city': self._safe_get(self.property_row, city_col) if city_col else 'N/A',
            'city_state': self._safe_get(self.property_row, 'State') if city_col else 'N/A',
            'business_type': self._safe_get(self.property_row, business_col) if business_col else 'N/A',
            'naics_code': self._safe_get(self.property_row, naics_col) if naics_col else 'N/A',
            'year_built': self._safe_get(self.property_row, year_col) if year_col else 'N/A',
            'tiv': self._format_currency(self.property_row.get(tiv_col, 0) if tiv_col else 0),
            'years_in_business': self._safe_get(self.property_row, years_biz_col) if years_biz_col else 'N/A',
            # 'years_in_business': (2025 - (self._safe_get(self.property_row, year_col)) if year_col else self._safe_get(self.property_row, years_biz_col) if years_biz_col else 'N/A'),
        }
    
    def _extract_building_details(self):
        """Extract building occupation summary"""
        # Use flexible column finding for better ACORD form compatibility
        df = self.input_df
        
        construction_col = self._find_column(df, ['Construction Type', 'Type of Construction', 'Building Construction'])
        stories_col = self._find_column(df, ['# of Stories', 'Number of Stories', 'Stories', 'Floors'])
        area_col = self._find_column(df, ['Total Area (Sq Ft)', 'Total Area', 'Square Footage', 'Area'])
        sprinkler_col = self._find_column(df, ['Sprinklered %', 'Sprinkler Coverage', 'Sprinklered Percent', 'Sprinkler %'])
        fire_class_col = self._find_column(df, ['Fire Protection Class', 'Fire Class', 'Protection Class'])
        alarm_col = self._find_column(df, ['Burglar Alarm Type', 'Burglar Alarm', 'Alarm Type', 'Security System'])
        roof_col = self._find_column(df, ['Verified Roof Condition', 'Roof Condition', 'Roof Age', 'Roof'])
        
        return {
            'construction_type': self._safe_get(self.property_row, construction_col) if construction_col else 'N/A',
            'stories': self._safe_get(self.property_row, stories_col) if stories_col else 'N/A',
            'total_area': self._safe_get(self.property_row, area_col) if area_col else 'N/A',
            'sprinklered_pct': self._format_percentage(self.property_row.get(sprinkler_col, 0) if sprinkler_col else 0),
            'fire_protection_class': self._safe_get(self.property_row, fire_class_col) if fire_class_col else 'N/A',
            'burglar_alarm': self._safe_get(self.property_row, alarm_col) if alarm_col else 'N/A',
            'roof_condition': self._safe_get(self.property_row, roof_col) if roof_col else 'N/A',
        }
    
    def _extract_risk_drivers(self):
        """Extract claim likelihood drivers from output data"""
        drivers = []
        
        # Get top risk factors from output
        if 'Top_Risk_Factors' in self.output_row.index:
            factors_str = self._safe_get(self.output_row, 'Top_Risk_Factors', '')
            if factors_str and factors_str != 'N/A':
                drivers = [f.strip() for f in factors_str.split('|') if f.strip()]
        
        # If no factors found, generate from risk scores
        if not drivers:
            if self.output_row.get('Property_Risk_Score', 0) > 60:
                drivers.append(f"High Property Risk Score: {self._format_percentage(self.output_row.get('Property_Risk_Score', 0))}")
            if self.output_row.get('Claims_Risk_Score', 0) > 60:
                drivers.append(f"Elevated Claims History Risk: {self._format_percentage(self.output_row.get('Claims_Risk_Score', 0))}")
            if self.output_row.get('Geographic_Risk_Score', 0) > 60:
                drivers.append(f"High Geographic Risk: {self._format_percentage(self.output_row.get('Geographic_Risk_Score', 0))}")
            if self.output_row.get('Protection_Risk_Score', 0) > 60:
                drivers.append(f"Inadequate Protection Systems: {self._format_percentage(self.output_row.get('Protection_Risk_Score', 0))}")
        
        return drivers if drivers else ["Standard risk profile - no significant adverse factors identified"]
    
    def _find_column(self, df, possible_names):
        """Find a column by checking multiple possible names (case-insensitive)"""
        if df is None or df.empty:
            return None
        
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        for name in possible_names:
            if name.lower() in df_columns_lower:
                return df_columns_lower[name.lower()]
        
        return None
    
    def _extract_risk_component_details(self):
        """Extract detailed information for each risk component"""
        # Use flexible column finding for risk component data
        df = self.input_df
        
        construction_col = self._find_column(df, ['Construction Type', 'Type of Construction', 'Building Construction'])
        year_col = self._find_column(df, ['Year Built', 'Construction Year', 'Year'])
        roof_col = self._find_column(df, ['Verified Roof Condition', 'Roof Condition', 'Roof Age', 'Roof'])
        sprinkler_col = self._find_column(df, ['Sprinklered %', 'Sprinkler Coverage', 'Sprinklered Percent', 'Sprinkler %'])
        
        details = {
            'Property': [
                f"Construction Type: {self._safe_get(self.property_row, construction_col) if construction_col else 'N/A'}",
                f"Year Built: {self._safe_get(self.property_row, year_col) if year_col else 'N/A'}",
                f"Roof Condition: {self._safe_get(self.property_row, roof_col) if roof_col else 'N/A'}",
                f"Sprinkler Coverage: {self._format_percentage(self.property_row.get(sprinkler_col, 0) if sprinkler_col else 0)}",
            ],
            'Claims History': [],
            'Geographic': [
                f"Wildfire Risk: {self._safe_get(self.output_row, 'Wildfire Risk Score', 'N/A')}",
                f"FEMA Flood Zone: {self._safe_get(self.output_row, 'FEMA Flood Zone', 'N/A')}",
                f"Earthquake Zone: {self._safe_get(self.output_row, 'Earthquake Zone', 'N/A')}",
                f"Crime Score: {self._safe_get(self.output_row, 'Crime Score', 'N/A')}",
            ],
            'Protection': [
                f"Fire Protection Class: {self._safe_get(self.property_row, self._find_column(df, ['Fire Protection Class', 'Fire Class', 'Protection Class'])) if self._find_column(df, ['Fire Protection Class', 'Fire Class', 'Protection Class']) else 'N/A'}",
                f"Burglar Alarm Type: {self._safe_get(self.property_row, self._find_column(df, ['Burglar Alarm Type', 'Burglar Alarm', 'Alarm Type', 'Security System'])) if self._find_column(df, ['Burglar Alarm Type', 'Burglar Alarm', 'Alarm Type', 'Security System']) else 'N/A'}",
                f"Fire Station Distance: {self._safe_get(self.output_row, 'Distance to Fire Station (miles)', 'N/A')} mi",
            ],
        }
        
        # Extract Claims History data from input_df
        loss_count_col = self._find_column(df, ['Loss History - Count', 'Claim Count', 'Claims Count', 'Loss Count'])
        loss_amount_col = self._find_column(df, ['Loss History - Total Amount', 'Total Loss Amount', 'Loss Amount', 'Total Amount'])
        loss_types_col = self._find_column(df, ['loss history', 'Loss History', 'Loss Types', 'Claim Types'])
        
        # Build Claims History details
        claim_count = self._safe_get(self.property_row, loss_count_col) if loss_count_col else 'N/A'
        loss_amount = self._safe_get(self.property_row, loss_amount_col) if loss_amount_col else 'N/A'
        loss_types_raw = self._safe_get(self.property_row, loss_types_col) if loss_types_col else 'N/A'
        
        # Extract Type from JSON loss history data if it exists
        loss_types = 'N/A'
        if loss_types_raw != 'N/A':
            try:
                import json
                # Try to parse as JSON
                loss_data = json.loads(loss_types_raw)
                # If it's a list, get the first item
                if isinstance(loss_data, list) and len(loss_data) > 0:
                    loss_data = loss_data[0]
                # Extract the Type field if it's a dict
                if isinstance(loss_data, dict) and 'Type' in loss_data:
                    loss_types = loss_data['Type']
                else:
                    loss_types = loss_types_raw
            except:
                # If JSON parsing fails, use the raw value
                loss_types = loss_types_raw
        
        # Format loss amount as currency if it's a number
        if loss_amount != 'N/A':
            loss_amount = self._format_currency(loss_amount)
        
        details['Claims History'] = [
            f"Claim Count: {claim_count}",
            f"Total Loss Amount: {loss_amount}",
            f"Loss Types: {loss_types}",
        ]
        
        return details
    
    def _find_column(self, df, possible_names):
        """Find a column by checking multiple possible names (case-insensitive)"""
        if df is None or df.empty:
            return None
        
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        for name in possible_names:
            if name.lower() in df_columns_lower:
                return df_columns_lower[name.lower()]
        
        return None
    
    def _generate_final_review(self):
        """Generate final review and recommendation (without final decision)"""
        overall_score = self.output_row.get('Overall_Risk_Score', 0)
        risk_level = self._safe_get(self.output_row, 'Risk_Level', 'UNKNOWN')
        recommendation = self._safe_get(self.output_row, 'Recommendation', 'Review required')
        
        # Build comprehensive review
        review_parts = []
        
        # Overall assessment
        review_parts.append(f"Overall Claims Likelihood Score: {self._format_percentage(overall_score)} ({risk_level})")
        review_parts.append(f"Underwriting Recommendation: {recommendation}")
        
        # Risk Component Analysis with details
        risk_components = self._extract_risk_component_details()
        
        review_parts.append("\n[RISK_COMPONENTS_START]")
        
        # Property Risk
        property_score = self.output_row.get('Property_Risk_Score', 0)
        property_line = f"Property Risk ({self._format_percentage(property_score)})"
        if 'Property' in risk_components and risk_components['Property']:
            property_line += "|" + "|".join(risk_components['Property'])
        review_parts.append(property_line)
        
        review_parts.append("")
        
        # Claims History Risk
        claims_score = self.output_row.get('Claims_Risk_Score', 0)
        claims_line = f"Claims History Risk ({self._format_percentage(claims_score)})"
        if 'Claims History' in risk_components and risk_components['Claims History']:
            claims_line += "|" + "|".join(risk_components['Claims History'])
        review_parts.append(claims_line)
        
        review_parts.append("")
        
        # Geographic Risk
        geo_score = self.output_row.get('Geographic_Risk_Score', 0)
        geo_line = f"Geographic Risk ({self._format_percentage(geo_score)})"
        if 'Geographic' in risk_components and risk_components['Geographic']:
            geo_line += "|" + "|".join(risk_components['Geographic'])
        review_parts.append(geo_line)
        
        review_parts.append("")
        
        # Protection Risk
        protection_score = self.output_row.get('Protection_Risk_Score', 0)
        protection_line = f"Protection Risk ({self._format_percentage(protection_score)})"
        if 'Protection' in risk_components and risk_components['Protection']:
            protection_line += "|" + "|".join(risk_components['Protection'])
        review_parts.append(protection_line)
        
        review_parts.append("[RISK_COMPONENTS_END]")
        
        return "\n".join(review_parts)
    
    def _generate_final_recommendation(self):
        """Generate the final recommendation decision"""
        overall_score = self.output_row.get('Overall_Risk_Score', 0)
        
        if overall_score < 45:
            return "This property presents a favorable risk profile and may qualify for auto-bind processing with standard terms."
        elif overall_score < 60:
            return "This property requires standard underwriting review. Consider appropriate premium adjustments based on identified risk factors."
        elif overall_score < 80:
            return "This property presents elevated risk and should be referred to senior underwriting for detailed evaluation. Enhanced monitoring and risk mitigation requirements are recommended."
        else:
            return "This property presents significant risk concerns. Recommend decline or referral to specialized underwriting team for enhanced terms evaluation."
    
    def generate_pdf(self, output_path: str = None):
        """Generate the complete PDF report"""
        if output_path is None:
            output_path = self.get_filename()
        
        # Create canvas
        c = canvas.Canvas(output_path, pagesize=A4)
        width, height = A4
        
        # Draw header
        y_pos = self._draw_header(c, width, height)
        y_pos -= 20
        
        # CLIENT DETAILS SECTION
        y_pos = self._draw_section_header(c, y_pos, "CLIENT & PROPERTY DETAILS", width)
        
        client_details = self._extract_client_details()
        city_state_val = f"{client_details['city_city']}, {client_details['city_state']}"
        client_data = [
            ('Client Name', client_details['client_name']),
            ('Property Address', client_details['property_address']),
            ('City/State', city_state_val),
            # ('Business Type', client_details['business_type']),
            ('NAICS Code', client_details['naics_code']),
            ('Year Built', client_details['year_built']),
            # ('Years in Business', client_details['years_in_business']),
            ('Total Insured Value (TIV)', client_details['tiv']),
        ]
        
        # y_pos = self._draw_key_value_section(c, y_pos, client_data)
        left_data = client_data[:3]
        right_data = client_data[3:]
        current_y = y_pos - 5
        
        for i in range(4):
            # Left column
            if i < len(left_data):
                label, value = left_data[i]
                c.setFont("Helvetica-Bold", 9)
                c.drawString(60, current_y, str(label) + ":")
                c.setFont("Helvetica", 9)
                c.drawString(200, current_y, str(value))
            
            # Right column
            if i < len(right_data):
                label, value = right_data[i]
                c.setFont("Helvetica-Bold", 9)
                c.drawString(width // 2 + 20, current_y, str(label) + ":")
                c.setFont("Helvetica", 9)
                c.drawString(width // 2 + 160, current_y, str(value))
            
            current_y -= 13
        
        y_pos = current_y - 5
        y_pos -= 10
        
        # BUILDING OCCUPATION SUMMARY SECTION
        y_pos = self._draw_section_header(c, y_pos, "BUILDING OCCUPATION SUMMARY", width)
        
        building_details = self._extract_building_details()
        building_data = [
            ('Construction Type', building_details['construction_type']),
            ('Number of Stories', building_details['stories']),
            ('Total Area', f"{building_details['total_area']} sq ft"),
            ('Fire Protection - Sprinklered', building_details['sprinklered_pct']),
            ('Fire Protection Class', building_details['fire_protection_class']),
            ('Burglar Alarm Type', building_details['burglar_alarm']),
            ('Roof Condition', building_details['roof_condition']),
        ]
        
        y_pos = self._draw_key_value_section(c, y_pos, building_data)
        y_pos -= 15
        
        # FINAL REVIEW SECTION - check if we need a new page
        if y_pos < 150:  # Not enough space for section header and content
            c.showPage()
            y_pos = height - 50
        
        y_pos = self._draw_section_header(c, y_pos, "UNDERWRITING REVIEW", width)
        
        final_review = self._generate_final_review()
        c.setFont("Helvetica", 9)
        
        review_lines = final_review.split('\n')
        in_risk_section = False
        
        for line in review_lines:
            if '[RISK_COMPONENTS_START]' in line:
                in_risk_section = True
                # Add "Risk Component Analysis:" header
                c.setFont("Helvetica-Bold", 9)
                c.drawString(60, y_pos, "Risk Component Analysis:")
                y_pos -= 14
                continue
            elif '[RISK_COMPONENTS_END]' in line:
                in_risk_section = False
                continue
            
            if line.strip():
                if in_risk_section:
                    # Two-column layout for risk components
                    if '|' in line:
                        parts = line.split('|')
                        left_part = parts[0].strip()
                        right_parts = [p.strip() for p in parts[1:] if p.strip()]
                        
                        # Left column - component name
                        c.setFont("Helvetica-Bold", 9)
                        c.drawString(60, y_pos, left_part + ":")
                        
                        # Right column - details (moved closer)
                        c.setFont("Helvetica", 8)
                        right_y = y_pos
                        for right_part in right_parts:
                            c.drawString(280, right_y, "• " + right_part)
                            right_y -= 11
                        
                        # Move y_pos down by max of left or right items
                        y_pos -= max(11, (len(right_parts) * 11) - 3)
                        # Add spacing between risk components
                        y_pos -= 8
                    else:
                        # Spacer line
                        y_pos -= 5
                else:
                    # Regular text
                    if line.strip().endswith(':'):
                        c.setFont("Helvetica-Bold", 9)
                    else:
                        c.setFont("Helvetica", 9)
                    
                    wrapped = self._wrap_text(line, 120)
                    for wrapped_line in wrapped:
                        c.drawString(60, y_pos, wrapped_line)
                        y_pos -= 13
                        
                        if y_pos < 100:
                            c.showPage()
                            y_pos = height - 50
        
        # FINAL RECOMMENDATION SECTION
        y_pos -= 15
        y_pos = self._draw_section_header(c, y_pos, "FINAL RECOMMENDATION", width)
        
        final_recommendation = self._generate_final_recommendation()
        c.setFont("Helvetica", 9)
        wrapped = self._wrap_text(final_recommendation, 120)
        for wrapped_line in wrapped:
            c.drawString(60, y_pos, wrapped_line)
            y_pos -= 14
            
            if y_pos < 100:
                c.showPage()
                y_pos = height - 50
        
        # Footer
        c.setFont("Helvetica", 7)
        c.drawString(50, 50, f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        c.drawRightString(width - 50, 50, "Confidential - For Underwriting Use Only")
        
        # Save PDF
        c.save()
        
        return output_path


# Standalone function for easy import
def generate_claims_likelihood_report(input_df, claims_df, output_df, output_path=None, logo_path=None):
    """
    Generate a claims likelihood analysis PDF report
    
    Args:
        input_df: Original property data DataFrame
        claims_df: Claims history DataFrame (can be None or empty)
        output_df: Analyzed output DataFrame with risk scores
        output_path: Optional custom output path
        logo_path: Optional path to company logo
    
    Returns:
        str: Path to generated PDF file
    """
    # Validate input
    if input_df is None or len(input_df) == 0:
        raise ValueError("input_df cannot be None or empty")
    
    if output_df is None or len(output_df) == 0:
        raise ValueError("output_df cannot be None or empty")
    
    # Handle empty claims_df
    if claims_df is None or len(claims_df) == 0:
        print("Warning: No claims data provided. Report will be generated without claims history.")
        claims_df = pd.DataFrame()
    logo_path = "./public/golden_bear.png"
    generator = ClaimsLikelihoodReportGenerator(input_df, claims_df, output_df, logo_path)
    return generator.generate_pdf(output_path)