# Atlan Metadata Manager - Modern UI

A modern, business-friendly UI tool for managing metadata in Atlan with AI-powered column description generation.

## Features

- **Modern React Frontend**: Built with React, TypeScript, and Tailwind CSS
- **AI-Powered Descriptions**: Generate column descriptions using Google Gemini AI
- **Multi-Source Support**: Works with PostgreSQL, Snowflake, and AWS S3
- **Intuitive Interface**: Clean, professional UI designed for business users
- **Real-time Updates**: Live feedback and status updates
- **Batch Operations**: Generate descriptions for all columns at once
- **Individual Control**: Generate descriptions for specific columns
- **Direct Atlan Integration**: Save descriptions directly to Atlan

## Architecture

### Backend (Flask)
- **Flask API**: RESTful API endpoints for data operations
- **Atlan Integration**: Direct integration with Atlan's Python SDK
- **AI Enhancement**: Google Gemini integration for description generation
- **Multi-source Connectors**: Support for various data sources

### Frontend (React)
- **React + TypeScript**: Type-safe, component-based architecture
- **Tailwind CSS**: Modern, responsive styling
- **TanStack Query**: Efficient API state management
- **Headless UI**: Accessible, unstyled components
- **React Hot Toast**: User-friendly notifications

## Setup Instructions

### Prerequisites
- Python 3.8+
- Node.js 16+
- npm or yarn

### Backend Setup

1. **Navigate to the modern_ui directory:**
   ```bash
   cd atlan_s3_connector/modern_ui
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   Create a `.env` file with the following variables:
   ```env
   ATLAN_BASE_URL=your_atlan_base_url
   ATLAN_API_KEY=your_atlan_api_key
   GOOGLE_API_KEY=your_google_api_key
   AWS_ACCESS_KEY_ID=your_aws_access_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   ```

### Frontend Setup

1. **Navigate to the frontend directory:**
   ```bash
   cd frontend
   ```

2. **Install Node.js dependencies:**
   ```bash
   npm install
   ```

3. **Build the React application:**
   ```bash
   npm run build
   ```

### Running the Application

1. **Start the Flask backend:**
   ```bash
   cd atlan_s3_connector/modern_ui
   python app.py
   ```

2. **Access the application:**
   Open your browser and navigate to `http://localhost:5001`

### Development Mode

For development with hot reloading:

1. **Start the Flask backend:**
   ```bash
   python app.py
   ```

2. **In a separate terminal, start the React dev server:**
   ```bash
   cd frontend
   npm run dev
   ```

3. **Access the development server:**
   Navigate to `http://localhost:3000`

## Usage Guide

### 1. Select Data Source
- Choose from PostgreSQL, AWS S3, or Snowflake
- The system will automatically fetch available assets

### 2. Select Asset/Table
- Pick the specific table or S3 object you want to work with
- View column information in the main panel

### 3. Generate Descriptions
- **Individual**: Click "Generate" on specific columns
- **Batch**: Use "Generate All" to process all columns at once
- AI will create business-friendly descriptions

### 4. Edit Descriptions
- Click on any description to edit manually
- Use keyboard shortcuts (Enter to save, Escape to cancel)

### 5. Save to Atlan
- Click "Save to Atlan" to persist changes
- Only modified columns will be updated

## API Endpoints

- `POST /api/assets_by_source` - Fetch assets for a data source
- `POST /api/columns` - Get columns for a specific asset
- `POST /api/enhance_columns` - Generate AI descriptions
- `POST /api/save_descriptions` - Save descriptions to Atlan

## Technology Stack

**Frontend:**
- React 18
- TypeScript
- Tailwind CSS
- Vite
- TanStack Query
- Headless UI
- Heroicons

**Backend:**
- Flask
- PyAtlan SDK
- Google Generative AI
- Boto3 (AWS S3)
- Python-dotenv

## Project Structure

```
modern_ui/
├── app.py                 # Flask application
├── config.py             # Configuration management
├── flask_ai_enhancer.py  # AI enhancement logic
├── s3_connector.py       # S3 integration
├── atlan_client.py       # Atlan client setup
├── requirements.txt      # Python dependencies
├── .env                  # Environment variables
└── frontend/             # React application
    ├── src/
    │   ├── components/   # React components
    │   ├── hooks/        # Custom hooks
    │   ├── services/     # API services
    │   ├── types/        # TypeScript types
    │   └── App.tsx       # Main application
    ├── package.json      # Node.js dependencies
    └── dist/             # Built application
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License.