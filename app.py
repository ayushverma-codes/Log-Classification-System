import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import FileResponse
import os

# Load environment variables
load_dotenv()

# Import your classify function
from pipeline.classify import classify

# Initialize FastAPI app
app = FastAPI()

@app.post("/classify/")
async def classify_logs(file: UploadFile):
    # Check file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV.")

    try:
        # Read uploaded CSV into DataFrame
        df = pd.read_csv(file.file)
        
        # Validate required columns
        if "source" not in df.columns or "log_message" not in df.columns:
            raise HTTPException(status_code=400, detail="CSV must contain 'source' and 'log_message' columns.")

        # Apply classification
        df["target_label"] = classify(list(zip(df["source"], df["log_message"])))

        # Save to output file
        output_file = r"D:\Projects\Log Classification System\Artifacts/classified_logs.csv"
        df.to_csv(output_file, index=False)

        print(f"[INFO] File classified and saved to: {output_file}")
        return FileResponse(output_file, media_type='text/csv', filename="classified_logs.csv")

    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        file.file.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)


