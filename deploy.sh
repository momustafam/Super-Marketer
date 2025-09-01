#!/bin/bash
# Deployment script for Super Marketer with RAG System

echo "🚀 Deploying Super Marketer with RAG System..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found. Creating from template..."
    cp .env.example .env
    echo "📝 Please update .env file with your API keys before continuing."
    echo "   Required: GROQ_API_KEY, REACT_APP_IMAGEKIT_* variables"
    read -p "Press Enter when .env is configured..."
fi

# Validate required environment variables
echo "🔍 Validating environment configuration..."
source .env

if [ -z "$GROQ_API_KEY" ] || [ "$GROQ_API_KEY" = "your_groq_api_key_here" ]; then
    echo "❌ GROQ_API_KEY is not set. Please update your .env file."
    exit 1
fi

echo "✅ Environment validation passed"

# Build and start services
echo "🔨 Building Docker images..."
docker-compose build

echo "🚀 Starting services..."
docker-compose up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
echo "   This may take a few minutes for databases to initialize..."

# Monitor service status
services=("sql_data_warehouse" "sql_marketing_data_mart" "chatbot_backend")
for service in "${services[@]}"; do
    echo "   Waiting for $service..."
    timeout=300  # 5 minutes
    elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        if docker-compose ps | grep $service | grep -q "healthy\|Up"; then
            echo "   ✅ $service is ready"
            break
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    if [ $elapsed -ge $timeout ]; then
        echo "   ⚠️  $service took longer than expected to start"
    fi
done

# Show status
echo ""
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "🎉 Deployment complete!"
echo ""
echo "Access your services:"
echo "   Frontend:              http://localhost:3000"
echo "   Chatbot API:           http://localhost:8001"
echo "   Backend API:           http://localhost:8000"
echo "   Airflow:               http://localhost:8080"
echo ""
echo "🔍 To monitor logs:"
echo "   All services:          docker-compose logs -f"
echo "   Chatbot only:          docker-compose logs -f chatbot_backend"
echo "   Database logs:         docker-compose logs -f data_warehouse marketing_data_mart"
echo ""
echo "🧪 To test the RAG system:"
echo "   docker-compose exec chatbot_backend python test_rag.py"
