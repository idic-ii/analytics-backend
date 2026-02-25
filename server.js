import Fastify from 'fastify'
import cors from '@fastify/cors'
import pg from 'pg'

const { Pool } = pg

const fastify = Fastify({ logger: true })

const normalizeOrigin = (value) => {
    if (!value) return ''
    const s = String(value).trim().replace(/^"|"$/g, '').replace(/^'|'$/g, '')
    return s.replace(/\/+$/, '')
}

const getOrigins = () => {
    const raw = process.env.CORS_ORIGIN
    if (!raw) return true
    const parts = raw.split(',').map(s => normalizeOrigin(s)).filter(Boolean)
    if (parts.length === 0) return true
    return parts
}

await fastify.register(cors, {
    origin: (origin, cb) => {
        const allowed = getOrigins()
        if (allowed === true) return cb(null, true)

        if (!origin) return cb(null, true)
        const reqOrigin = normalizeOrigin(origin)
        if (Array.isArray(allowed) && allowed.includes(reqOrigin)) return cb(null, true)

        return cb(null, false)
    },
    credentials: true,
    methods: ['POST', 'GET', 'OPTIONS'],
})

if (!process.env.DATABASE_URL) {
    fastify.log.error('Missing DATABASE_URL. Configure a Postgres database and set DATABASE_URL in environment variables.')
    throw new Error('Missing DATABASE_URL')
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.PGSSL === 'true' || process.env.PGSSL === '1' ? { rejectUnauthorized: false } : undefined,
})

const requireStatsAuth = (request, reply) => {
    const token = process.env.STATS_TOKEN
    if (!token) return true

    const header = request.headers['authorization'] || ''
    if (typeof header === 'string' && header.toLowerCase().startsWith('bearer ')) {
        const provided = header.slice(7).trim()
        if (provided === token) return true
    }

    const apiKey = request.headers['x-api-key']
    if (typeof apiKey === 'string' && apiKey === token) return true

    reply.code(401).send({ ok: false, error: 'unauthorized' })
    return false
}

const toInt = (v, fallback) => {
    const n = Number(v)
    if (!Number.isFinite(n)) return fallback
    return Math.trunc(n)
}

const initDb = async () => {
    const client = await pool.connect()
    try {
        await client.query(`
            CREATE TABLE IF NOT EXISTS events (
                id bigserial PRIMARY KEY,
                created_at timestamptz NOT NULL DEFAULT now(),
                type text NOT NULL,
                name text NULL,
                path text NULL,
                title text NULL,
                referrer text NULL,
                session_id text NULL,
                utm_source text NULL,
                utm_medium text NULL,
                utm_campaign text NULL,
                utm_term text NULL,
                utm_content text NULL,
                data jsonb NOT NULL DEFAULT '{}'::jsonb,
                ip inet NULL,
                user_agent text NULL
            );
        `)
        await client.query(`CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at DESC);`)
        await client.query(`CREATE INDEX IF NOT EXISTS idx_events_type_name ON events (type, name);`)
    } finally {
        client.release()
    }
}

await initDb()

fastify.get('/health', async () => {
    return { ok: true }
})

fastify.get('/stats/overview', async (request, reply) => {
    if (!requireStatsAuth(request, reply)) return

    const days = Math.max(1, Math.min(365, toInt(request.query?.days, 30)))

    const client = await pool.connect()
    try {
        const { rows } = await client.query(
            `
            SELECT
                COUNT(*) FILTER (WHERE type = 'page_view')::bigint AS page_views,
                COUNT(*) FILTER (WHERE type = 'event')::bigint AS events,
                COUNT(DISTINCT session_id) FILTER (WHERE session_id IS NOT NULL)::bigint AS sessions
            FROM events
            WHERE created_at >= now() - ($1::int * interval '1 day');
            `,
            [days]
        )
        return {
            ok: true,
            window_days: days,
            page_views: Number(rows?.[0]?.page_views || 0),
            events: Number(rows?.[0]?.events || 0),
            sessions: Number(rows?.[0]?.sessions || 0),
        }
    } finally {
        client.release()
    }
})

fastify.get('/stats/timeseries', async (request, reply) => {
    if (!requireStatsAuth(request, reply)) return

    const days = Math.max(1, Math.min(365, toInt(request.query?.days, 30)))
    const type = request.query?.type === 'event' ? 'event' : 'page_view'
    const name = typeof request.query?.name === 'string' ? request.query.name : null

    const client = await pool.connect()
    try {
        const params = [days]
        let where = `created_at >= now() - ($1::int * interval '1 day') AND type = $2`
        params.push(type)

        if (type === 'event' && name) {
            where += ` AND name = $3`
            params.push(name)
        }

        const { rows } = await client.query(
            `
            SELECT
                date_trunc('day', created_at) AS day,
                COUNT(*)::bigint AS count
            FROM events
            WHERE ${where}
            GROUP BY 1
            ORDER BY 1 ASC;
            `,
            params
        )

        return {
            ok: true,
            window_days: days,
            type,
            name: type === 'event' ? name : null,
            series: rows.map(r => ({ day: r.day, count: Number(r.count) })),
        }
    } finally {
        client.release()
    }
})

fastify.get('/stats/top-events', async (request, reply) => {
    if (!requireStatsAuth(request, reply)) return

    const days = Math.max(1, Math.min(365, toInt(request.query?.days, 30)))
    const limit = Math.max(1, Math.min(100, toInt(request.query?.limit, 10)))

    const client = await pool.connect()
    try {
        const { rows } = await client.query(
            `
            SELECT
                name,
                COUNT(*)::bigint AS count
            FROM events
            WHERE created_at >= now() - ($1::int * interval '1 day')
              AND type = 'event'
              AND name IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT $2::int;
            `,
            [days, limit]
        )

        return {
            ok: true,
            window_days: days,
            items: rows.map(r => ({ name: r.name, count: Number(r.count) })),
        }
    } finally {
        client.release()
    }
})

fastify.post('/collect', async (request, reply) => {
    const body = request.body || {}

    const type = typeof body.type === 'string' ? body.type : null
    const name = typeof body.name === 'string' ? body.name : null

    if (!type || (type !== 'page_view' && type !== 'event')) {
        return reply.code(400).send({ ok: false, error: 'invalid_type' })
    }

    const path = typeof body.path === 'string' ? body.path : null
    const title = typeof body.title === 'string' ? body.title : null
    const referrer = typeof body.referrer === 'string' ? body.referrer : null
    const session_id = typeof body.session_id === 'string' ? body.session_id : null

    const utm_source = typeof body.utm_source === 'string' ? body.utm_source : null
    const utm_medium = typeof body.utm_medium === 'string' ? body.utm_medium : null
    const utm_campaign = typeof body.utm_campaign === 'string' ? body.utm_campaign : null
    const utm_term = typeof body.utm_term === 'string' ? body.utm_term : null
    const utm_content = typeof body.utm_content === 'string' ? body.utm_content : null

    const data = { ...body }
    delete data.type
    delete data.name
    delete data.path
    delete data.title
    delete data.referrer
    delete data.session_id
    delete data.utm_source
    delete data.utm_medium
    delete data.utm_campaign
    delete data.utm_term
    delete data.utm_content

    const user_agent = request.headers['user-agent'] || null
    const ip = request.ip || null

    const client = await pool.connect()
    try {
        await client.query(
            `
            INSERT INTO events (
                type, name, path, title, referrer, session_id,
                utm_source, utm_medium, utm_campaign, utm_term, utm_content,
                data, ip, user_agent
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11,
                $12::jsonb, $13::inet, $14
            )
            `,
            [
                type,
                name,
                path,
                title,
                referrer,
                session_id,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_term,
                utm_content,
                JSON.stringify(data || {}),
                ip,
                user_agent,
            ]
        )
    } finally {
        client.release()
    }

    return reply.code(204).send()
})

const port = process.env.PORT ? Number(process.env.PORT) : 10000
const host = '0.0.0.0'

await fastify.listen({ port, host })
