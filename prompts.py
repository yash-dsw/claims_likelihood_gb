"""
Prompts and templates for the Predictive Underwriting POC
"""

SYSTEM_PROMPT = """You are an Insurance Underwriting AI Assistant.

Available tools:
1. analyze_property_risks - Analyze the uploaded property
2. get_property_details - Get details for the property
3. list_all_properties - Show property risk score

RULES:
- Call ONE tool per request, then return the result to the user
- Display tool output EXACTLY as returned (it's pre-formatted)
- Do NOT call the same tool multiple times
- After calling a tool, immediately respond with its output"""

RISK_ANALYSIS_PROMPT = """Analyze the following property risk assessment data and provide a comprehensive summary:

{risk_data}

Explain for this property:
1. The overall claim likelihood and recommendation
2. Key risk factors contributing to the score
3. Any areas of concern that require attention

Format your response in a clear, professional manner suitable for an underwriting report."""

SUMMARY_PROMPT = """Based on the risk analysis of the property, provide an executive summary:

Risk Assessment:
- Overall Score: {average_score}%
- Risk Level: {risk_level}

Total TIV Exposure: ${total_tiv:,.2f}

Provide insights on:
1. Key drivers of claim likelihood
2. Recommendations for the underwriting team"""

ANALYSIS_SUMMARY_PROMPT = """You are a Senior Lead Underwriter. Write a risk summary in a specific 2-part format.

**Property Profile:**
- Risk Level: {risk_level} ({overall_score}%)
- Critical Factors: {top_factors}
- Category Scores: P:{property_risk}% C:{claims_risk}% G:{geographic_risk}% S:{protection_risk}%

**Instructions:**
1. **Part 1 (The Narrative):** Write a bullet-pointwise summary (3-4 sentences). 
   - Start with: "This property has a **[Risk Level] claim likelihood** with an overall score of **{overall_score}%**."
   - Mention the primary risk drivers.
   - Conclude with the underwriting recommendation (e.g., "This property can proceed through standard underwriting...").
   - Bold key terms.

2. **Part 2 (The Actions):**
   - Skip a line.
   - Write exactly: "**Suggested Actions:**"
   - Provide a bulleted list of 3-4 specific, high-impact requirements or recommendations based on the top factors (e.g., "Require proof of upgraded fire protection", "Mandate sprinkler installation").
   - Do NOT use any markdown headers (like ###). Use only bold text for emphasis.
"""


# =============================================================================
# GENERAL DATA QUERY PROMPTS
# =============================================================================

DATA_QUERY_CLASSIFICATION_PROMPT = """Analyze this user query and determine if it's asking about the uploaded property data.

User Query: "{user_query}"

Available columns in the data:
{columns}

Respond with ONLY "YES" if the query is asking about the data (counts, averages, specific properties, filtering, comparisons, etc.)
Respond with ONLY "NO" if it's a general question not related to the data.

Answer:"""


DATA_QUERY_PLANNING_PROMPT = """You are a data analyst. Generate Python pandas code to answer this question about a DataFrame called 'df'.

**User Question:** {user_query}

**DataFrame Schema:**
{schema}

**Sample Data (first 3 rows):**
{sample_data}

**Current Year:** {current_year} (Use this for age calculations: Age = {current_year} - Year Built)

**instructions:**
1. Write ONLY the pandas code to get the answer
2. Store the result in a variable called 'result'
3. The code should be a single expression or a few lines maximum
4. Use only: filtering, groupby, value_counts, mean, sum, count, max, min, sort_values, head, shape
5. Do NOT use: eval, exec, import, open, file operations, system calls
6. **CRITICAL:** For text searching/filtering:
   - ALWAYS search the 'Street Address' column for location-based questions.
   - ALWAYS use `.astype(str).str.contains(..., case=False, na=False)` for robust matching.
   - NEVER use exact equality (`==`) for addresses.

**IMPORTANT:** 
- If the user asks to LIST or SHOW properties matching a criteria, return the property details (Name, Score, etc), NOT just a count
- If asking "how many", return the count
- For filtering queries like "properties with score > X", return a list with relevant columns like Street Address, the score column, Risk Level

**Examples:**
- "How many high risk properties?" → result = df[df['Risk_Level'] == 'HIGH'].shape[0]
- "Average TIV?" → result = df['TIV (Total Insurable Value)'].mean()
- "Properties by construction type?" → result = df['Construction Type'].value_counts().to_dict()
- "Show properties with overall risk > 50" → result = df[df['Overall_Risk_Score'] > 50][['Street Address', 'Overall_Risk_Score', 'Risk_Level']].to_dict('records')
- "List high risk properties" → result = df[df['Risk_Level'] == 'HIGH'][['Street Address', 'Overall_Risk_Score', 'Risk_Level']].to_dict('records')
- "How many claims for 35 Lien Point?" → result = df[df['Street Address'].astype(str).str.contains('35 lien point', case=False, na=False)]['Loss History - Count'].sum()
- "Claims for Nelson Lane" → result = df[df['Street Address'].astype(str).str.contains('nelson lane', case=False, na=False)]['Loss History - Count'].sum()

**Your pandas code (ONLY the code, no explanation):**
"""


DATA_QUERY_RESPONSE_PROMPT = """Answer the user's question based on the query results.

**User Question:** {user_query}

**Query Result:** {result}

**Instructions:**
- Give a direct answer without phrases like "According to our analysis" or "Based on the data"
- Start with the key information immediately
- If the result is a list of properties, format as a markdown table with columns
- If it's a single number, state it clearly with context
- Format large numbers with commas (e.g., $1,234,567)
- For lists, show all items in a clean format
- **Format all risk scores as percentages** (e.g., 65.5 becomes 65.5%)

**Example formats:**
- For counts: "There are 5 properties with overall risk score above 50%:"
- For lists: Show a table with Property Name, Score (as %), Risk Level
- For averages: "The average TIV is $2,500,000."

**Your response:**"""


INTENT_CLASSIFICATION_PROMPT = """Classify the user's intent from their message.

User Message: "{user_message}"

Available intents:
1. ANALYZE - User wants to run risk analysis on the uploaded property (e.g., "analyze", "run analysis", "assess risk", "summary")
2. DATA_QUERY - User is asking a specific question that requires querying/filtering the data (e.g., "what is the building age", "how many claims", "TIV for this property", "details of fire loss")
3. DOWNLOAD - User wants to download the data (e.g., "download", "export")
4. PROPERTY_DETAILS - User wants general details of the property (e.g., "show property details", "details")
5. GENERAL - General question not about data (e.g., "what is underwriting", "hello")

IMPORTANT: 
1. **DATA_QUERY (Prioritize this):** 
   - Use this if the user asks a SPECIFIC question about a value, attribute, or statistic.
   - Examples: "building age", "claim count", "TIV value", "construction type".

2. **PROPERTY_DETAILS:**
   - Use this for broad requests to see the property card/profile.

Respond with ONLY the intent name (e.g., "DATA_QUERY" or "ANALYZE"), nothing else."""
