/**
 * MapViewer - Interactive French Real Estate Price Map
 * 
 * Uses official French government vector tiles from:
 * https://openmaptiles.geo.data.gouv.fr
 * 
 * Multi-level visualization:
 * - Zoom 0-5:  Country outline
 * - Zoom 5-7:  Regions (with stats)
 * - Zoom 7-9:  Departments (with stats)
 * - Zoom 9-11: Cantons (with stats)
 * - Zoom 11+:  Communes (dynamically loaded by department)
 */

// ===========================
// Configuration
// ===========================

const API_BASE = '';

// Geographic layers with zoom ranges (for GeoJSON layers with stats)
// Note: communes are loaded dynamically by department, not from a single file
const GEO_LAYERS = [
    { id: 'country', file: 'country.geojson', minzoom: 0, maxzoom: 5 },
    { id: 'regions', file: 'regions.geojson', minzoom: 5, maxzoom: 7 },
    { id: 'departements', file: 'departements.geojson', minzoom: 7, maxzoom: 9 },
    { id: 'cantons', file: 'cantons.geojson', minzoom: 9, maxzoom: 11 },
];

// Track loaded commune departments to avoid re-fetching
const loadedCommuneDepts = new Set();

// Price color interpolation (MapLibre expression)
const PRICE_COLOR_EXPR = [
    'interpolate',
    ['linear'],
    ['coalesce', ['get', 'price_m2'], 3000],
    1000, '#27ae60',  // Green - cheap
    4000, '#f1c40f',  // Yellow
    7000, '#e67e22',  // Orange
    12000, '#ff0000', // Bright Red - expensive
];

// ===========================
// Map Initialization with French Government Vector Tiles
// ===========================

const map = new maplibregl.Map({
    container: 'map',
    style: {
        version: 8,
        name: 'MapViewer Style',
        sources: {
            // OpenMapTiles base layer from French government
            'openmaptiles': {
                type: 'vector',
                url: 'https://openmaptiles.geo.data.gouv.fr/data/planet-vector.json'
            },
            // French administrative boundaries (communes, departements)
            'decoupage-administratif': {
                type: 'vector',
                url: 'https://openmaptiles.geo.data.gouv.fr/data/decoupage-administratif.json'
            },
            // Fallback raster for areas without vector coverage
            'osm-raster': {
                type: 'raster',
                tiles: [
                    'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
                    'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
                    'https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
                ],
                tileSize: 256,
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
            },
            // IGN Cadastral parcels (WMTS - pre-cached tiles, better rate limits)
            'cadastre': {
                type: 'raster',
                tiles: [
                    'https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&LAYER=CADASTRALPARCELS.PARCELLAIRE_EXPRESS&STYLE=normal&FORMAT=image/png&TILEMATRIXSET=PM&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}'
                ],
                tileSize: 256,
                minzoom: 14,
                maxzoom: 19,
                attribution: '&copy; <a href="https://www.ign.fr">IGN</a>'
            },
        },
        sprite: 'https://openmaptiles.github.io/osm-bright-gl-style/sprite',
        glyphs: 'https://openmaptiles.geo.data.gouv.fr/fonts/{fontstack}/{range}.pbf',
        layers: [
            // Background
            {
                id: 'background',
                type: 'background',
                paint: { 'background-color': '#f5f5f5' }
            },
            // Fallback raster tiles
            {
                id: 'osm-raster-tiles',
                type: 'raster',
                source: 'osm-raster',
                minzoom: 0,
                maxzoom: 19,
                paint: { 'raster-opacity': 0.5 }
            },
            // Water from OpenMapTiles
            {
                id: 'water',
                type: 'fill',
                source: 'openmaptiles',
                'source-layer': 'water',
                paint: { 'fill-color': '#a0c8f0' }
            },
            // Land use
            {
                id: 'landuse-residential',
                type: 'fill',
                source: 'openmaptiles',
                'source-layer': 'landuse',
                filter: ['in', 'class', 'residential', 'suburb', 'neighbourhood'],
                paint: { 'fill-color': 'hsla(30, 19%, 90%, 0.4)' }
            },
            // Roads
            {
                id: 'highway-motorway',
                type: 'line',
                source: 'openmaptiles',
                'source-layer': 'transportation',
                minzoom: 5,
                filter: ['==', 'class', 'motorway'],
                paint: {
                    'line-color': '#fc8',
                    'line-width': ['interpolate', ['linear'], ['zoom'], 6, 0.5, 20, 18]
                }
            },
            {
                id: 'highway-primary',
                type: 'line',
                source: 'openmaptiles',
                'source-layer': 'transportation',
                minzoom: 8,
                filter: ['in', 'class', 'primary', 'trunk'],
                paint: {
                    'line-color': '#fea',
                    'line-width': ['interpolate', ['linear'], ['zoom'], 8, 0.5, 20, 18]
                }
            },
            // Place labels
            {
                id: 'place-city',
                type: 'symbol',
                source: 'openmaptiles',
                'source-layer': 'place',
                filter: ['==', 'class', 'city'],
                layout: {
                    'text-field': '{name:latin}',
                    'text-font': ['Noto Sans Regular'],
                    'text-size': ['interpolate', ['linear'], ['zoom'], 7, 14, 11, 24]
                },
                paint: {
                    'text-color': '#333',
                    'text-halo-color': 'rgba(255,255,255,0.8)',
                    'text-halo-width': 1.2
                }
            },
            {
                id: 'place-town',
                type: 'symbol',
                source: 'openmaptiles',
                'source-layer': 'place',
                minzoom: 10,
                filter: ['==', 'class', 'town'],
                layout: {
                    'text-field': '{name:latin}',
                    'text-font': ['Noto Sans Regular'],
                    'text-size': ['interpolate', ['linear'], ['zoom'], 10, 12, 15, 22]
                },
                paint: {
                    'text-color': '#333',
                    'text-halo-color': 'rgba(255,255,255,0.8)',
                    'text-halo-width': 1.2
                }
            },
            // Administrative boundaries from decoupage-administratif
            {
                id: 'communes-outline',
                type: 'line',
                source: 'decoupage-administratif',
                'source-layer': 'communes',
                minzoom: 10,
                maxzoom: 16,
                paint: {
                    'line-opacity': 0.4,
                    'line-color': '#666',
                    'line-width': 0.5
                }
            },
            {
                id: 'departements-outline-vector',
                type: 'line',
                source: 'decoupage-administratif',
                'source-layer': 'departements',
                minzoom: 7,
                paint: {
                    'line-opacity': 0.5,
                    'line-color': '#333',
                    'line-width': 1
                }
            },
            // IGN Cadastral parcels (WMS raster tiles)
            {
                id: 'cadastre-layer',
                type: 'raster',
                source: 'cadastre',
                minzoom: 14,
                maxzoom: 19,
                paint: {
                    'raster-opacity': 0.7
                }
            },
        ],
    },
    center: [2.3522, 46.6034],
    zoom: 5,
    minZoom: 4,
    maxZoom: 19,
});

map.addControl(new maplibregl.NavigationControl(), 'top-right');
map.addControl(new maplibregl.ScaleControl({ maxWidth: 200 }), 'bottom-right');

// ===========================
// Stats Cache
// ===========================

let statsCache = null;

async function loadStatsCache() {
    try {
        const response = await fetch('/stats_cache.json');
        if (response.ok) {
            statsCache = await response.json();
            console.log('Stats cache loaded:', Object.keys(statsCache));
        }
    } catch (error) {
        console.warn('Could not load stats cache:', error);
    }
}

function getStatsForCode(level, code) {
    if (!statsCache || !statsCache[level]) return null;
    return statsCache[level][code] || null;
}

// ===========================
// Layer Loading (GeoJSON with stats)
// ===========================

async function addGeoLayer(layerConfig) {
    const { id, file, minzoom, maxzoom } = layerConfig;

    try {
        const response = await fetch(`/${file}`);
        if (!response.ok) {
            console.warn(`Could not load ${file}`);
            return;
        }
        const geoData = await response.json();

        // Enrich features with price data from stats cache
        const statsLevel = id === 'departements' ? 'departement' :
            id === 'regions' ? 'region' :
                id === 'cantons' ? 'canton' : 'country';

        geoData.features.forEach(feature => {
            const code = feature.properties.code;
            const stats = getStatsForCode(statsLevel, code);
            if (stats) {
                feature.properties.price_m2 = stats.median_price_m2;
                feature.properties.n_sales = stats.n_sales;
                feature.properties.q25 = stats.q25;
                feature.properties.q75 = stats.q75;
            }
        });
        // Add source
        map.addSource(id, {
            type: 'geojson',
            data: geoData
        });

        // Add fill layer (below the vector tile outlines)
        map.addLayer({
            id: `${id}-fill`,
            type: 'fill',
            source: id,
            minzoom: minzoom,
            maxzoom: maxzoom,
            paint: {
                'fill-color': PRICE_COLOR_EXPR,
                'fill-opacity': 0.6,
            },
        }, 'communes-outline'); // Insert below outline layers

        // Add outline layer
        map.addLayer({
            id: `${id}-outline`,
            type: 'line',
            source: id,
            minzoom: minzoom,
            maxzoom: maxzoom,
            paint: {
                'line-color': '#333',
                'line-width': id === 'country' ? 2 : 1,
            },
        });

        console.log(`Added layer: ${id} (${geoData.features.length} features)`);

    } catch (error) {
        console.warn(`Error loading ${file}:`, error);
    }
}

// ===========================
// Map Load Handler
// ===========================

map.on('load', async () => {
    console.log('Map loaded with French government vector tiles');

    // Load stats cache first (needed for communes dynamic loading)
    await loadStatsCache();

    // Load all geographic layers in parallel for faster startup
    await Promise.all(GEO_LAYERS.map(layerConfig => addGeoLayer(layerConfig)));

    // Initialize communes source (empty, will be populated dynamically)
    map.addSource('communes', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] }
    });

    // Add communes fill layer
    map.addLayer({
        id: 'communes-fill',
        type: 'fill',
        source: 'communes',
        minzoom: 11,
        maxzoom: 17,
        paint: {
            'fill-color': PRICE_COLOR_EXPR,
            'fill-opacity': 0.7,
        },
    }, 'communes-outline');

    // Add communes outline layer
    map.addLayer({
        id: 'communes-line',
        type: 'line',
        source: 'communes',
        minzoom: 11,
        maxzoom: 17,
        paint: {
            'line-color': '#333',
            'line-width': 0.5,
        },
    });

    console.log('Communes layer ready for dynamic loading');

    // Hide loading overlay
    document.getElementById('loading').classList.add('hidden');

    // Load initial visible communes
    loadVisibleCommunes();
});

// ===========================
// Dynamic Commune Loading
// ===========================

// Department bounding boxes (approximated from departements.geojson)
let deptBounds = null;

async function loadDeptBounds() {
    try {
        const response = await fetch('/departements.geojson');
        const data = await response.json();
        deptBounds = {};

        data.features.forEach(f => {
            const code = f.properties.code;
            // Calculate bounding box from geometry
            const coords = [];
            const extractCoords = (geom) => {
                if (Array.isArray(geom[0])) {
                    geom.forEach(extractCoords);
                } else {
                    coords.push(geom);
                }
            };
            extractCoords(f.geometry.coordinates);

            const lons = coords.map(c => c[0]);
            const lats = coords.map(c => c[1]);
            deptBounds[code] = {
                minLon: Math.min(...lons),
                maxLon: Math.max(...lons),
                minLat: Math.min(...lats),
                maxLat: Math.max(...lats),
            };
        });
        console.log(`Loaded bounds for ${Object.keys(deptBounds).length} departments`);
    } catch (e) {
        console.warn('Could not load dept bounds:', e);
    }
}

function getVisibleDepartments() {
    if (!deptBounds) return [];

    const bounds = map.getBounds();
    const visible = [];

    for (const [code, bbox] of Object.entries(deptBounds)) {
        // Check if department bbox intersects with viewport
        if (bbox.maxLon >= bounds.getWest() &&
            bbox.minLon <= bounds.getEast() &&
            bbox.maxLat >= bounds.getSouth() &&
            bbox.minLat <= bounds.getNorth()) {
            visible.push(code);
        }
    }
    return visible;
}

async function loadCommuneDept(deptCode) {
    if (loadedCommuneDepts.has(deptCode)) return;

    try {
        const response = await fetch(`/communes/${deptCode}.geojson`);
        if (!response.ok) return;

        const data = await response.json();
        loadedCommuneDepts.add(deptCode);

        // Get current source data and merge
        const source = map.getSource('communes');
        const currentData = source._data || { type: 'FeatureCollection', features: [] };

        // Merge new features (already enriched with stats by split script)
        const newFeatures = [...currentData.features, ...data.features];
        source.setData({ type: 'FeatureCollection', features: newFeatures });

        console.log(`Loaded communes for dept ${deptCode}: ${data.features.length} features`);
    } catch (e) {
        console.warn(`Error loading communes for ${deptCode}:`, e);
    }
}

async function loadVisibleCommunes() {
    const zoom = map.getZoom();
    if (zoom < 10) return; // Preload at zoom 10, before communes become visible at 11

    if (!deptBounds) {
        await loadDeptBounds();
    }

    const visible = getVisibleDepartments();
    const toLoad = visible.filter(d => !loadedCommuneDepts.has(d));

    if (toLoad.length > 0) {
        console.log(`Loading ${toLoad.length} department(s): ${toLoad.join(', ')}`);
        await Promise.all(toLoad.map(loadCommuneDept));
    }
}

// Load communes when zooming or panning at high zoom (debounced)
let communeLoadTimeout;
map.on('moveend', () => {
    clearTimeout(communeLoadTimeout);
    communeLoadTimeout = setTimeout(loadVisibleCommunes, 150);
});


// ===========================
// Hover Interaction
// ===========================

const hoverInfoEl = document.getElementById('hover-info');

// Helper to format price
function formatPrice(price) {
    if (!price) return 'N/A';
    return Math.round(price).toLocaleString() + ' EUR/m2';
}

// Add hover handlers for each layer
function addHoverHandler(layerId, displayName) {
    map.on('mousemove', `${layerId}-fill`, (e) => {
        if (e.features.length > 0) {
            const props = e.features[0].properties;
            map.getCanvas().style.cursor = 'pointer';

            const interval = props.q25 && props.q75
                ? `${formatPrice(props.q25)} - ${formatPrice(props.q75)}`
                : 'N/A';

            hoverInfoEl.innerHTML = `
                <div class="stat">
                    <span class="stat-label">${displayName}</span>
                    <span class="stat-value">${props.name || props.nom || props.code || 'N/A'}${props.name && props.code ? ` (${props.code})` : ''}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Prix median</span>
                    <span class="stat-value price">${formatPrice(props.price_m2)}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Intervalle</span>
                    <span class="stat-value">${interval}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Transactions</span>
                    <span class="stat-value">${props.n_sales ? props.n_sales.toLocaleString() : 'N/A'}</span>
                </div>
            `;
        }
    });

    map.on('mouseleave', `${layerId}-fill`, () => {
        map.getCanvas().style.cursor = '';
        hoverInfoEl.innerHTML = '<p class="hint">Survolez la carte pour voir les details</p>';
    });
}

// Register hover handlers after map loads
map.on('load', () => {
    addHoverHandler('country', 'Pays');
    addHoverHandler('regions', 'Region');
    addHoverHandler('departements', 'Departement');
    addHoverHandler('cantons', 'Canton');
    addHoverHandler('communes', 'Commune');
    // Note: Cadastre layer uses WMS raster tiles - no hover interaction available
});

// ===========================
// Zoom Level Display
// ===========================

const zoomLevelEl = document.getElementById('zoom-level');

map.on('zoom', () => {
    const zoom = map.getZoom().toFixed(1);
    if (zoomLevelEl) zoomLevelEl.innerText = zoom;
});

// Note: Cadastre tiles are now loaded directly from IGN's WMS service

// ===========================
// Error Handling
// ===========================

map.on('error', (e) => {
    console.error('Map error:', e);
});

// ===========================
// Top 10 Communes
// ===========================
async function loadTopCommunes() {
    try {
        const response = await fetch('/top_expensive.json');
        if (!response.ok) return;
        const data = await response.json();
        const listEl = document.getElementById('top-communes-list');

        if (data.data && data.data.length > 0) {
            listEl.innerHTML = data.data.map((c, i) => `
                <li>
                    <span>${i + 1}. ${c.city}</span>
                    <span class="price">${c.median_price_m2.toLocaleString()} EUR</span>
                </li>
            `).join('');
        } else {
            listEl.innerHTML = '<li class="hint">Aucune donnee disponible</li>';
        }
    } catch (e) {
        console.warn('Error loading top communes:', e);
    }
}

// Call on load
loadTopCommunes();
