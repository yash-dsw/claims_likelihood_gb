"""
Predictive Underwriting POC - Chainlit Application
Simple direct flow for risk analysis
"""

import os
import chainlit as cl
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from dotenv import load_dotenv

from utils import (
    load_property_data,
    load_claims_data,
    process_all_properties,
    generate_summary_stats,
    format_property_summary,
    format_aggregate_summary,
    add_risk_scores_to_df,
    general_data_query,
    smart_load_data
)

# Load environment variables
load_dotenv()

# Global storage for uploaded data
uploaded_data = {
    "property_df": None,
    "claims_df": None,
    "results": None,
    "scored_df": None
}


def get_llm():
    """Get LLM instance"""
    return ChatOpenAI(
        model="meta-llama/llama-3.3-70b-instruct",
        api_key=os.getenv("OPENROUTER_API_KEY"),
        base_url="https://openrouter.ai/api/v1",
        temperature=0.3,
    )


async def run_analysis():
    """Run the complete risk analysis and return formatted output"""
    if uploaded_data["property_df"] is None:
        return "❌ No property data uploaded. Please upload your data file first.", None
    
    property_df = uploaded_data["property_df"]
    claims_df = uploaded_data.get("claims_df")
    
    # Add risk scores to DataFrame
    scored_df = add_risk_scores_to_df(property_df, claims_df)
    uploaded_data["scored_df"] = scored_df
    
    # Process all properties for detailed results
    # Use scored_df to ensure we use the dataframe with populated defaults
    results = process_all_properties(scored_df, claims_df)
    uploaded_data["results"] = results

    # Get the single property result (assuming single property upload as per requirement)
    if results:
        single_result = results[0]
        # Get LLM for summary generation
        llm = get_llm()
        summary = format_property_summary(single_result, llm)
    else:
        summary = "❌ No properties found in the data."

    return summary, results



def get_property_details(search_term: str) -> str:
    """Get details for a specific property by name or address"""
    if uploaded_data["results"] is None:
        return "❌ Please run the analysis first by typing 'analyze'."
    
    results = uploaded_data["results"]
    property_df = uploaded_data["property_df"]
    search_lower = search_term.lower()
    
    # First, try to find by address (Mailing Address or Street Address)
    matching_by_address = []
    if property_df is not None:
        for idx, row in property_df.iterrows():
            mailing_addr = str(row.get('Mailing Address', '')).lower()
            street_addr = str(row.get('Street Address', '')).lower()
            
            # Check if search term matches any part of either address
            if search_lower in mailing_addr or search_lower in street_addr:
                # Find the corresponding result by Agency Customer ID or index
                agency_id = row.get('Agency Customer ID')
                for r in results:
                    if r.get('agency_customer_id') == agency_id:
                        matching_by_address.append(r)
                        break
                else:
                    # Fallback: match by index if agency_id not in results
                    if idx < len(results):
                        matching_by_address.append(results[idx])
    
    llm = get_llm()
    
    if matching_by_address:
        output = ""
        for result in matching_by_address:
            output += format_property_summary(result, llm)
        return output
    
    # Fallback: Search by property name (case-insensitive partial match)
    matching_by_name = [r for r in results if search_lower in r['named_insured'].lower()]
    
    # If all properties have the same name, we need address to differentiate
    if len(matching_by_name) > 1 and len(set(r['named_insured'] for r in matching_by_name)) == 1:
        return f"❌ Multiple properties found with name '{matching_by_name[0]['named_insured']}'. Please specify an address (e.g., 'show details for Logan Lane' or 'property at 59 Randy Place')."
    
    if not matching_by_name:
        return f"❌ No property found matching '{search_term}'. Try searching by address (e.g., 'Logan Lane' or '59 Randy Place')."
    
    output = ""
    for result in matching_by_name:
        output += format_property_summary(result, llm)
    
    return output

from pdf_gen import generate_claims_likelihood_report

async def generate_pdf_report():
    """Generate PDF report from analyzed data"""
    if uploaded_data["property_df"] is None:
        return "❌ No property data uploaded.", None
    
    if uploaded_data["scored_df"] is None:
        return "❌ Please run analysis first before generating report.", None
    
    try:
        # Get the dataframes
        input_df = uploaded_data["scored_df"]
        claims_df = uploaded_data.get("claims_df")
        output_df = uploaded_data["scored_df"]
        
        # Generate the PDF report
        # Optional: Add your company logo path
        logo_path = None  # Set to your logo file path if available
        
        pdf_path = generate_claims_likelihood_report(
            input_df=input_df,
            claims_df=claims_df if claims_df is not None else pd.DataFrame(),
            output_df=output_df,
            logo_path=logo_path
        )
        
        return pdf_path, None
        
    except Exception as e:
        import traceback
        error_msg = f"Error generating PDF: {str(e)}\n{traceback.format_exc()}"
        return None, error_msg

def get_actions():
    actions = [
        cl.Action(name="action_analyze", label="📈 Analyze Claim Likelihood", value="analyze_data", payload={"value": "analyze_data"}),
        cl.Action(name="action_download", label="📋 Download Report", value="download_data", payload={"value": "download_data"}),
    ]
    return actions

# =============================================================================
# CHAINLIT EVENT HANDLERS
# =============================================================================

@cl.action_callback("action_analyze")
async def on_action_analyze(action: cl.Action):
    """Handle analyze action button click with streaming output"""
    actions = get_actions()
    
    # Create message for streaming
    msg = cl.Message(content="🔄 Analyzing property...")
    await msg.send()
    
    # Run analysis
    summary, results = await run_analysis()
    
    # Stream the output progressively
    # Split the summary into sections for progressive display
    import asyncio
    
    # Clear the loading message and start streaming
    msg.content = ""
    
    # Stream the content in chunks for a smooth experience
    chunk_size = 12  # characters per chunk (smaller = slower, more visible)
    for i in range(0, len(summary), chunk_size):
        chunk = summary[i:i + chunk_size]
        await msg.stream_token(chunk)
        # Delay for visible streaming effect (higher = slower)
        await asyncio.sleep(0.1)
    
    # Update with actions
    msg.actions = actions
    await msg.update()


@cl.action_callback("action_download")
async def on_action_download(action: cl.Action):
    """Handle download action button click"""
    msg = cl.Message(content="📄 Generating comprehensive PDF report...")
    await msg.send()
    
    pdf_path, error = await generate_pdf_report()
    
    if error:
        msg.content = f"❌ Failed to generate PDF report. Please try again"
        msg.actions = get_actions()
        await msg.update()
        print("PDF GENERATION ERROR:", error)
    elif pdf_path and os.path.exists(pdf_path):
        # Send the PDF as a downloadable file
        elements = [cl.File(name=os.path.basename(pdf_path), path=pdf_path)]
        msg.content = f"✅ **PDF Report Generated Successfully!**\n\nYour comprehensive claims likelihood report is ready for download."
        msg.elements = elements
        msg.actions = get_actions()
        await msg.update()
    else:
        msg.content = "❌ PDF generation failed - file not created"
        msg.actions = get_actions()
        await msg.update()

@cl.on_chat_start
async def on_chat_start():
    """Initialize the chat session"""
    
    await cl.Message(
        content="""#### 🏢 Predictive Underwriting For Claims Likelihood

Welcome! I analyze property data and provide underwriting recommendations.

📁 **Getting Started**:
1. Upload your application form
2. Perform claim likelihood analysis
3. Get underwriting recommendations
4. Download the assessment pdf

**Upload your application form to begin.**
        """
    ).send()


@cl.on_message
async def on_message(message: cl.Message):
    """Handle incoming messages"""
    
    
    # Check for file uploads
    if message.elements:
        print(f"DEBUG: Received {len(message.elements)} elements")
        for element in message.elements:
            print(f"DEBUG: Element type: {type(element).__name__}")
            print(f"DEBUG: Attrs: path={getattr(element, 'path', None)}, url={getattr(element, 'url', None)}, name={getattr(element, 'name', None)}")
            
            file_path = None
            file_name = getattr(element, 'name', 'uploaded_file.csv')
            
            # Method 1: Direct path (older Chainlit versions)
            if hasattr(element, 'path') and element.path:
                file_path = element.path
                print(f"DEBUG: Using direct path: {file_path}")
            
            # Method 2: URL-based (Chainlit 2.8.0+)
            elif hasattr(element, 'url') and element.url:
                import tempfile
                import httpx
                
                temp_dir = tempfile.gettempdir()
                file_path = os.path.join(temp_dir, file_name)
                print(f"DEBUG: Downloading from URL to: {file_path}")
                
                try:
                    # Download file from URL
                    response = httpx.get(element.url, timeout=30)
                    response.raise_for_status()
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    print(f"DEBUG: Downloaded {len(response.content)} bytes")
                except Exception as e:
                    print(f"DEBUG: Download failed: {e}")
                    await cl.Message(content=f"❌ Failed to download file: {e}").send()
                    return
            
            # Method 3: Content-based
            elif hasattr(element, 'content') and element.content:
                import tempfile
                import base64
                
                temp_dir = tempfile.gettempdir()
                file_path = os.path.join(temp_dir, file_name)
                print(f"DEBUG: Saving content to: {file_path}")
                
                with open(file_path, 'wb') as f:
                    if isinstance(element.content, bytes):
                        f.write(element.content)
                    else:
                        f.write(base64.b64decode(element.content))
            else:
                print(f"DEBUG: Element has no path, url, or content!")
                continue
            
            if file_path and os.path.exists(file_path):
                try:
                    # Smartly load data using our utility function
                    data_dict = smart_load_data(file_path)
                    
                    msg_content_list = []
                    
                    # Collect property name and claims count
                    prop_name = None
                    claims_count = None
                    
                    if 'property_df' in data_dict:
                        uploaded_data["property_df"] = data_dict['property_df']
                        print("PROPERTY DATA===============", uploaded_data["property_df"].columns)
                        uploaded_data["results"] = None
                        uploaded_data["scored_df"] = None
                        
                        # Get property name safely
                        if not uploaded_data["property_df"].empty:
                            # Try standard column names
                            for col in ['Named Insured', 'Property Name', 'Insured Name', 'Company']:
                                if col in uploaded_data["property_df"].columns:
                                    prop_name = str(uploaded_data["property_df"].iloc[0][col])
                                    break
                        
                        if not prop_name:
                            prop_name = "Unknown Property"
                        
                    if 'claims_df' in data_dict:
                        uploaded_data["claims_df"] = data_dict['claims_df']
                        uploaded_data["results"] = None 
                        uploaded_data["scored_df"] = None
                        claims_count = len(uploaded_data['claims_df'])
                    
                    # Create simple message with property name and claims count
                    if prop_name and claims_count is not None:
                        message = f"✅ **Data uploaded!**\n\n**Property:** {prop_name}\n**Claims Found:** {claims_count}"
                    elif prop_name:
                        message = f"✅ **Data uploaded!**\n\n**Property:** {prop_name}"
                    elif claims_count is not None:
                        message = f"✅ **Data uploaded!**\n\n**Claims Found:** {claims_count}"
                    else:
                        message = f"⚠️ **Uploaded file {file_name} but could not automatically identify data type.**"

                    await cl.Message(
                        content=message + "\n\n*Click 'Analyze Claim Likelihood' to proceed.*",
                        actions = get_actions()
                    ).send()
                    
                except Exception as e:
                    import traceback
                    await cl.Message(content=f"❌ Error loading file: {str(e)}\n\nDetails: {traceback.format_exc()[:500]}").send()
        
        # Return after processing all files
        return
    
    # Process commands using LLM-based intent classification
    user_input = message.content.strip()
    user_input_lower = user_input.lower()
    
    # Quick shortcuts for exact/simple commands (no LLM needed)
    if user_input_lower in ["analyze", "analyse", "run analysis", "summary", "summarize"]:
        import asyncio
        
        msg = cl.Message(content="🔄 Analyzing property...")
        await msg.send()
        result, _ = await run_analysis()
        
        # Stream the output progressively
        msg.content = ""
        chunk_size = 20  # characters per chunk (smaller = slower, more visible)
        for i in range(0, len(result), chunk_size):
            chunk = result[i:i + chunk_size]
            await msg.stream_token(chunk)
            await asyncio.sleep(0.03)
        
        await msg.update()
        return
    
    if user_input_lower == "download":
        if uploaded_data["scored_df"] is not None:
            output_path = "risk_scored_properties.csv"
            uploaded_data["scored_df"].to_csv(output_path, index=False)
            elements = [cl.File(name="risk_scored_properties.csv", path=output_path)]
            await cl.Message(
                content="📥 **Download ready!** The file includes the property with calculated risk score columns.",
                elements=elements
            ).send()
        else:
            await cl.Message(content="❌ Run analysis first before downloading.").send()
        return
    
    # For all other queries, use LLM to classify intent
    try:
        llm = get_llm()
        
        # Import the intent classification prompt
        from prompts import INTENT_CLASSIFICATION_PROMPT
        
        # Classify intent (no streaming needed for quick classification)
        intent_prompt = INTENT_CLASSIFICATION_PROMPT.format(user_message=user_input)
        intent_response = llm.invoke([HumanMessage(content=intent_prompt)])
        intent = intent_response.content.strip().upper()
        
        # Route based on intent
        if intent == "ANALYZE" or intent == "SUMMARY" or intent == "LIST_ALL":
            import asyncio
            
            msg = cl.Message(content="🔄 Analyzing property...")
            await msg.send()
            result, _ = await run_analysis()
            
            # Stream the output progressively
            msg.content = ""
            chunk_size = 20  # characters per chunk (smaller = slower, more visible)
            for i in range(0, len(result), chunk_size):
                chunk = result[i:i + chunk_size]
                await msg.stream_token(chunk)
                await asyncio.sleep(0.03)
            
            await msg.update()
                
        elif intent == "DOWNLOAD":
            if uploaded_data["scored_df"] is not None:
                output_path = "risk_scored_properties.csv"
                uploaded_data["scored_df"].to_csv(output_path, index=False)
                elements = [cl.File(name="risk_scored_properties.csv", path=output_path)]
                await cl.Message(
                    content="📥 **Download ready!** The file includes the property with calculated risk score columns.",
                    elements=elements
                ).send()
            else:
                await cl.Message(content="❌ Run analysis first before downloading.").send()
                
        elif intent == "PROPERTY_DETAILS":
            if uploaded_data["results"]:
                property_df = uploaded_data["property_df"]
                
                # Get list of addresses for extraction
                addresses = []
                if property_df is not None:
                    for idx, row in property_df.iterrows():
                        mailing = row.get('Mailing Address', '')
                        street = row.get('Street Address', '')
                        if mailing:
                            addresses.append(f"Mailing: {mailing}")
                        if street:
                            addresses.append(f"Street: {street}")
                

                
                # Extract address or search term from user query
                extract_prompt = f"""From this query, extract the address or property identifier the user is asking about: "{user_input}"

Available addresses in the data (sample):
{chr(10).join(addresses[:20])}

Instructions:
1. Extract the address or location reference from the user's query
2. Common patterns: "Logan Lane", "59 Randy Place", "Warrior Center", etc.
3. Return ONLY the address/street name, nothing else
4. Do not include "show me", "details for", etc. - just the address/identifier

Extracted address/identifier:"""
                
                response = llm.invoke([HumanMessage(content=extract_prompt)])
                extracted_term = response.content.strip().strip('"').strip("'")
                
                # Clean up the extracted term
                extracted_term = extracted_term.replace("Mailing:", "").replace("Street:", "").strip()
                
                # Try finding property by extracted term
                result = get_property_details(extracted_term)
                
                # If not found, try extracting address parts from user input directly
                if "No property found" in result or "Multiple properties found" in result:
                    # Extract likely address terms from user input
                    user_query_lower = user_input.lower()
                    for pattern in ["show me about", "show me the details for", "show me details for", 
                                    "show me", "details for", "details of", "details", "about", 
                                    "get", "property at", "property on", "the"]:
                        user_query_lower = user_query_lower.replace(pattern, "").strip()
                    
                    # Try this cleaned query
                    if user_query_lower:
                        result = get_property_details(user_query_lower)
                
                await cl.Message(content=result).send()
            else:
                await cl.Message(content="❌ Please run 'analyze' first to load property data.").send()
                
        elif intent == "DATA_QUERY":
            # Handle data query with streaming
            
            # Determine which dataframe to use based on keywords
            target_df = None
            is_claims_query = any(k in user_input.lower() for k in ['claim', 'loss', 'accident', 'incident'])
            
            if is_claims_query and uploaded_data["claims_df"] is not None:
                target_df = uploaded_data["claims_df"]
            elif uploaded_data["scored_df"] is not None:
                target_df = uploaded_data["scored_df"]
            elif uploaded_data["property_df"] is not None:
                target_df = uploaded_data["property_df"]
                
            if target_df is not None:
                # Import streaming utility
                from utils import general_data_query_streaming
                
                # Create message for streaming
                msg = cl.Message(content="")
                await msg.send()
                
                # Stream the response
                await general_data_query_streaming(
                    target_df,
                    user_input,
                    llm,
                    msg
                )
            else:
                await cl.Message(content="❌ Please upload data first.").send()
                
        else:  # GENERAL or unknown intent - use streaming
            context = ""
            if uploaded_data["results"]:
                r = uploaded_data["results"][0]
                context = f"""
Current Property Analysis:
- Address: {r['address']}
- Overall Claim Likelihood: {r['overall_score']:.1f}% ({r['risk_level']})
- TIV: ${r['tiv']:,.2f}
- Property Risk: {r['property_risk']:.1f}%
- Claims History Risk: {r['claims_risk']:.1f}%
- Recommendation: {r['recommendation']}
"""
            
            messages = [
                SystemMessage(content=f"""You are an insurance underwriting assistant. 
Help the user understand the claim likelihood analysis for the uploaded property.

{context}

Available commands: analyze, download
Keep responses concise."""),
                HumanMessage(content=user_input)
            ]
            
            # Stream the response
            msg = cl.Message(content="")
            await msg.send()
            
            async for chunk in llm.astream(messages):
                if chunk.content:
                    await msg.stream_token(chunk.content)
            
            await msg.update()
            
    except Exception as e:
        import traceback
        await cl.Message(
            content=f"Commands: **analyze**, **download**\n\n(Error: {str(e)})"
        ).send()



if __name__ == "__main__":
    print("Run with: chainlit run app.py")
