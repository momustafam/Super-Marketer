# Deployment script for Super Marketer with RAG System (PowerShell)

Write-Host "🚀 Deploying Super Marketer with RAG System..." -ForegroundColor Green

# Check if .env file exists
if (!(Test-Path ".env")) {
    Write-Host "⚠️  .env file not found. Creating from template..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host "📝 Please update .env file with your API keys before continuing." -ForegroundColor Yellow
    Write-Host "   Required: GROQ_API_KEY, REACT_APP_IMAGEKIT_* variables" -ForegroundColor White
    Read-Host "Press Enter when .env is configured"
}

# Load environment variables
Write-Host "🔍 Validating environment configuration..." -ForegroundColor Yellow
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2])
        }
    }
}

$groqApiKey = [Environment]::GetEnvironmentVariable("GROQ_API_KEY")
if ([string]::IsNullOrEmpty($groqApiKey) -or $groqApiKey -eq "your_groq_api_key_here") {
    Write-Host "❌ GROQ_API_KEY is not set. Please update your .env file." -ForegroundColor Red
    exit 1
}

Write-Host "✅ Environment validation passed" -ForegroundColor Green

# Build and start services
Write-Host "🔨 Building Docker images..." -ForegroundColor Yellow
docker-compose build

Write-Host "🚀 Starting services..." -ForegroundColor Yellow
docker-compose up -d

# Wait for services to be healthy
Write-Host "⏳ Waiting for services to be ready..." -ForegroundColor Yellow
Write-Host "   This may take a few minutes for databases to initialize..." -ForegroundColor White

# Monitor service status
$services = @("sql_data_warehouse", "sql_marketing_data_mart", "chatbot_backend")
foreach ($service in $services) {
    Write-Host "   Waiting for $service..." -ForegroundColor White
    $timeout = 300  # 5 minutes
    $elapsed = 0
    
    while ($elapsed -lt $timeout) {
        $status = docker-compose ps | Select-String $service
        if ($status -and ($status -match "healthy" -or $status -match "Up")) {
            Write-Host "   ✅ $service is ready" -ForegroundColor Green
            break
        }
        Start-Sleep 5
        $elapsed += 5
    }
    
    if ($elapsed -ge $timeout) {
        Write-Host "   ⚠️  $service took longer than expected to start" -ForegroundColor Yellow
    }
}

# Show status
Write-Host ""
Write-Host "📊 Service Status:" -ForegroundColor Cyan
docker-compose ps

Write-Host ""
Write-Host "🎉 Deployment complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Access your services:" -ForegroundColor Cyan
Write-Host "   Frontend:              http://localhost:3000" -ForegroundColor White
Write-Host "   Chatbot API:           http://localhost:8001" -ForegroundColor White
Write-Host "   Backend API:           http://localhost:8000" -ForegroundColor White
Write-Host "   Airflow:               http://localhost:8080" -ForegroundColor White
Write-Host ""
Write-Host "🔍 To monitor logs:" -ForegroundColor Cyan
Write-Host "   All services:          docker-compose logs -f" -ForegroundColor White
Write-Host "   Chatbot only:          docker-compose logs -f chatbot_backend" -ForegroundColor White
Write-Host "   Database logs:         docker-compose logs -f data_warehouse marketing_data_mart" -ForegroundColor White
Write-Host ""
Write-Host "🧪 To test the RAG system:" -ForegroundColor Cyan
Write-Host "   docker-compose exec chatbot_backend python test_rag.py" -ForegroundColor White
