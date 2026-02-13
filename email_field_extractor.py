"""
Email Field Extractor using LLM
Extracts broker and underwriter information from email metadata
"""

import os
import json
import requests
from dotenv import load_dotenv
from prompts import EMAIL_EXTRACTION_PROMPT

load_dotenv()


class EmailFieldExtractor:
    """Extract structured fields from email metadata using LLM"""
    
    def __init__(self):
        """Initialize the extractor with OpenRouter"""
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found in environment variables")
        
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self.model = os.getenv("OPENROUTER_MODEL", "anthropic/claude-3.5-sonnet")
    
    def extract_fields(self, email_metadata: dict) -> dict:
        """
        Extract broker and underwriter fields from email metadata.
        
        Args:
            email_metadata: Dictionary containing email data with keys:
                - from: sender email/name
                - toRecipients: recipient email(s)
                - subject: email subject line
                - bodyPreview or body: email body content
        
        Returns:
            Dictionary with extracted fields:
            {
                "sender_email": str,
                "sender_name": str,
                "receiver_email": str,
                "receiver_name": str,
                "policy_number": str,
                "agency_name": str,
                "agency_id": str,
                "email_summary": str
            }
        """
        if not email_metadata:
            return self._empty_result()
        
        # Extract email components
        from_field = email_metadata.get("from", "")
        to_field = email_metadata.get("toRecipients", "")
        subject = email_metadata.get("subject", "")
        body = email_metadata.get("bodyPreview", email_metadata.get("body", ""))
        
        # Handle list format for recipients
        if isinstance(to_field, list):
            to_field = ", ".join(to_field) if to_field else ""
        
        # Create prompt with email data
        prompt = EMAIL_EXTRACTION_PROMPT.format(
            from_email=from_field,
            to_email=to_field,
            subject=subject,
            body=body
        )
        
        try:
            # Call OpenRouter API
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.model,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 1024
            }
            
            response = requests.post(self.base_url, headers=headers, json=payload)
            response.raise_for_status()
            
            # Extract JSON from response
            response_data = response.json()
            response_text = response_data["choices"][0]["message"]["content"].strip()
            
            # Parse JSON
            extracted_data = json.loads(response_text)
            
            # Add aliases for backward compatibility
            extracted_data['broker_email'] = extracted_data.get('sender_email', 'Not Found')
            extracted_data['broker_name'] = extracted_data.get('sender_name', 'Not Found')
            extracted_data['underwriter_email'] = extracted_data.get('receiver_email', 'Not Found')
            extracted_data['underwriter_name'] = extracted_data.get('receiver_name', 'Not Found')
            extracted_data['broker_agency_name'] = extracted_data.get('agency_name', 'Not Found')
            extracted_data['broker_agency_id'] = extracted_data.get('agency_id', 'Not Found')
            
            print(f"✓ Email fields extracted successfully")
            print(f"  Sender: {extracted_data.get('sender_name', 'N/A')} ({extracted_data.get('sender_email', 'N/A')})")
            print(f"  Receiver: {extracted_data.get('receiver_name', 'N/A')} ({extracted_data.get('receiver_email', 'N/A')})")
            print(f"  Policy: {extracted_data.get('policy_number', 'N/A')}")
            print(f"  Summary: {extracted_data.get('email_summary', 'N/A')[:80]}..." if extracted_data.get('email_summary') else "  Summary: N/A")
            
            return extracted_data
            
        except json.JSONDecodeError as e:
            print(f"✗ Failed to parse LLM response as JSON: {str(e)}")
            print(f"  Response: {response_text}")
            return self._empty_result()
        
        except Exception as e:
            print(f"✗ Email field extraction error: {str(e)}")
            return self._empty_result()
    
    def _empty_result(self) -> dict:
        """Return empty result structure"""
        return {
            "sender_email": "Not Found",
            "sender_name": "Not Found",
            "receiver_email": "Not Found",
            "receiver_name": "Not Found",
            "policy_number": "Not Found",
            "agency_name": "Not Found",
            "agency_id": "Not Found",
            "email_summary": "Not Found",
            # Backward compatibility aliases
            "broker_email": "Not Found",
            "broker_name": "Not Found",
            "underwriter_email": "Not Found",
            "underwriter_name": "Not Found",
            "broker_agency_name": "Not Found",
            "broker_agency_id": "Not Found",
            "comments": "",
            "timestamp": ""
        }


def extract_email_fields(email_metadata: dict) -> dict:
    """
    Convenience function to extract email fields.
    
    Args:
        email_metadata: Email metadata dictionary
    
    Returns:
        Dictionary with extracted fields
    """
    extractor = EmailFieldExtractor()
    return extractor.extract_fields(email_metadata)


if __name__ == "__main__":
    # Test with sample data
    sample_email = {
        "from": "john.broker@abcinsurance.com",
        "toRecipients": ["sarah.underwriter@goldenbear.com"],
        "subject": "Submission for Policy ABC123456 - Commercial Property",
        "bodyPreview": "Dear Sarah, Please find attached the ACORD form for ABC Insurance Agency (ID: AG-9876). This is for renewal of policy ABC123456. Best regards, John Smith"
    }
    
    result = extract_email_fields(sample_email)
    print("\n" + "="*70)
    print("EXTRACTED FIELDS:")
    print("="*70)
    print(json.dumps(result, indent=2))
