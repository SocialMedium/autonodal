# MitchelLake Signal Intelligence Platform

Transform market noise into actionable candidate intelligence, matched to client needs in real-time.

## 🚀 Quick Start

### Prerequisites
- Node.js 18+
- PostgreSQL (Railway recommended)
- Qdrant Cloud account
- OpenAI API key

### Setup

1. **Install dependencies**
```bash
npm install
```

2. **Configure environment**
```bash
cp .env.example .env
# Edit .env with your credentials:
# - DATABASE_URL (PostgreSQL connection string)
# - QDRANT_URL (Qdrant Cloud cluster URL)
# - QDRANT_API_KEY (Qdrant API key)
# - OPENAI_API_KEY (OpenAI API key)
# - JWT_SECRET (Random string for auth)
```

3. **Initialize database**
```bash
npm run init-db
```

4. **Initialize Qdrant collections**
```bash
npm run init-qdrant
```

5. **Start the server**
```bash
npm run dev
```

6. **Open in browser**
```
http://localhost:3000
```

## 📁 Project Structure

```
mitchellake-signals/
├── server.js           # Express API server
├── package.json        # Dependencies
├── .env.example        # Environment template
│
├── lib/
│   ├── db.js           # PostgreSQL connection
│   ├── embeddings.js   # OpenAI embeddings
│   ├── qdrant.js       # Vector search client
│   └── signal_keywords.js  # Signal detection rules
│
├── scripts/
│   ├── init_db.js      # Database setup
│   ├── init_qdrant.js  # Vector DB setup
│   ├── harvest_rss.js  # RSS feed ingestion
│   ├── embed_documents.js  # Document embedding
│   ├── compute_signals.js  # Signal detection
│   ├── compute_scores.js   # Person scoring
│   └── match_searches.js   # AI matching
│
├── public/
│   ├── index.html      # Dashboard
│   ├── signals.html    # Signal triage
│   ├── people.html     # People list
│   ├── person.html     # Person dossier
│   ├── searches.html   # Search pipeline
│   └── companies.html  # Company list
│
└── sql/
    └── schema.sql      # Full database schema
```

## 🔧 Available Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start server with auto-reload |
| `npm run start` | Start production server |
| `npm run init-db` | Initialize PostgreSQL schema |
| `npm run init-qdrant` | Create Qdrant collections |
| `npm run harvest` | Fetch RSS feeds |
| `npm run embed` | Generate document embeddings |
| `npm run signals` | Detect signals from documents |
| `npm run scores` | Compute person scores |
| `npm run match` | Run search-candidate matching |
| `npm run pipeline` | Run full data pipeline |

## 📊 Data Pipeline

```
RSS Feeds → harvest_rss.js
    ↓
Documents → embed_documents.js → Qdrant (vectors)
    ↓
Signals → compute_signals.js → signal_events table
    ↓
People → compute_scores.js → person_scores table
    ↓
Searches → match_searches.js → search_matches table
```

Run the full pipeline:
```bash
npm run pipeline
```

## 🔌 API Endpoints

### Authentication
- `POST /api/auth/signup` - Register new user
- `POST /api/auth/login` - Login
- `POST /api/auth/logout` - Logout
- `GET /api/auth/me` - Current user

### Signals
- `GET /api/signals/brief` - Signal feed
- `GET /api/signals/:id` - Signal detail
- `PATCH /api/signals/:id/triage` - Update status

### People
- `GET /api/people` - List/search people
- `GET /api/people/:id` - Person detail
- `POST /api/people` - Create person
- `GET /api/people/:id/signals` - Person signals
- `POST /api/people/:id/interactions` - Log interaction

### Companies
- `GET /api/companies` - List companies
- `GET /api/companies/:id` - Company detail

### Searches
- `GET /api/searches` - Active searches
- `GET /api/searches/:id` - Search detail
- `POST /api/searches` - Create search
- `GET /api/searches/:id/candidates` - Pipeline
- `GET /api/searches/:id/matches` - AI suggestions

### Stats
- `GET /api/stats` - Dashboard statistics

## 🎯 Signal Types

### Company Signals
- `capital_raising` - Funding announcements
- `strategic_hiring` - Key executive hires
- `geographic_expansion` - New market entry
- `ma_activity` - M&A activity
- `product_launch` - New products
- `partnership` - Strategic partnerships
- `layoffs` - Workforce reductions
- `restructuring` - Organizational changes
- `leadership_change` - Executive departures

### Person Signals
- `job_change` - New role
- `promotion` - Career advancement
- `speaking_engagement` - Conference appearances
- `publication` - Thought leadership
- `award` - Recognition
- `board_appointment` - Board roles

## 📈 Scoring System

### Person Scores (0-1 scale)
- **Engagement** - Response rate, interaction depth
- **Activity** - External signals, content published
- **Receptivity** - Tenure, company stability
- **Flight Risk** - Company trouble, short tenure
- **Timing** - Overall opportunity window

### Match Scoring
- Vector similarity (40%)
- Experience match (20%)
- Skills match (15%)
- Location match (10%)
- Timing score (15%)

## 🗄️ Database

The schema includes 47 tables covering:
- **Demand side**: clients, projects, searches, pipelines
- **Supply side**: people, signals, scores, content
- **Market intelligence**: companies, signal_events, documents
- **Vectors**: embeddings, search_matches
- **Users**: auth, sessions, audit

## 🚢 Deployment

### Railway (Recommended)
1. Push to GitHub
2. Connect to Railway
3. Add PostgreSQL service
4. Set environment variables
5. Deploy

### Environment Variables
```env
DATABASE_URL=postgresql://...
QDRANT_URL=https://...
QDRANT_API_KEY=...
OPENAI_API_KEY=sk-...
JWT_SECRET=...
PORT=3000
NODE_ENV=production
```

## 📄 License

Private - MitchelLake Internal Use Only

---

Built with ❤️ for MitchelLake
