"""
Script to list available Gemini models
"""
import os
import google.generativeai as genai

# Get API key
api_key = os.getenv('GEMINI_API_KEY')
if not api_key:
    print("Error: GEMINI_API_KEY environment variable not set")
    print("Please set it with: export GEMINI_API_KEY='your-key-here'")
    exit(1)

# Configure Gemini
genai.configure(api_key=api_key)

# List models
print("Fetching available models...\n")
try:
    models = genai.list_models()
    
    print("Available models that support generateContent:\n")
    for model in models:
        if 'generateContent' in model.supported_generation_methods:
            print(f"  - {model.name}")
            if hasattr(model, 'display_name'):
                print(f"    Display Name: {model.display_name}")
            print()
    
    print("\nRecommended models for this app:")
    print("  - gemini-1.5-pro")
    print("  - gemini-1.5-flash")
    print("  - gemini-pro (if available)")
    
except Exception as e:
    print(f"Error listing models: {str(e)}")
    print("\nTroubleshooting:")
    print("1. Verify your API key is correct")
    print("2. Check your internet connection")
    print("3. Ensure the google-generativeai package is installed: pip3 install google-generativeai")

