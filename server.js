import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import os from 'os';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

let db = null;
let connection = null;
let currentFile = null;

async function initDB() {
  if (!db) {
    db = await DuckDBInstance.create(':memory:');
    connection = await db.connect();
  }
  return connection;
}

async function loadFile(filePath) {
  const conn = await initDB();
  const ext = path.extname(filePath).toLowerCase();
  
  try {
    await conn.run(`DROP TABLE IF EXISTS data`);
    
    if (ext === '.csv') {
      await conn.run(`CREATE TABLE data AS SELECT * FROM read_csv_auto('${filePath}')`);
    } else if (ext === '.parquet') {
      await conn.run(`CREATE TABLE data AS SELECT * FROM read_parquet('${filePath}')`);
    } else {
      throw new Error('Unsupported file type. Use .csv or .parquet');
    }
    
    currentFile = filePath;
    return { success: true };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

async function getData(whereClause = null) {
  const conn = await initDB();
  if (!currentFile) return { columns: [], rows: [] };
  
  try {
    let sql = 'SELECT * FROM data';
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }
    sql += ' LIMIT 1000';
    
    const result = await conn.runAndReadAll(sql);
    const columns = result.columnNames();
    const rows = result.getRows().map(row => 
      row.map(cell => typeof cell === 'bigint' ? Number(cell) : cell)
    );
    return { columns, rows };
  } catch (error) {
    return { columns: [], rows: [], error: error.message };
  }
}

async function getSummary() {
  const conn = await initDB();
  if (!currentFile) return { rowCount: 0, columns: [] };
  
  try {
    // Get row count
    const countResult = await conn.runAndReadAll('SELECT COUNT(*) as cnt FROM data');
    const rowCount = Number(countResult.getRows()[0][0]);
    
    // Get column info
    const schemaResult = await conn.runAndReadAll('DESCRIBE data');
    const columnInfo = schemaResult.getRows();
    
    const columns = [];
    
    for (const [colName, colType] of columnInfo) {
      const colStats = {
        name: colName,
        type: colType,
        nullCount: 0
      };
      
      // Get null count
      const nullResult = await conn.runAndReadAll(`SELECT COUNT(*) - COUNT("${colName}") as null_cnt FROM data`);
      colStats.nullCount = Number(nullResult.getRows()[0][0]);
      
      const typeLower = colType.toLowerCase();
      
      if (typeLower.includes('int') || typeLower.includes('bigint') || typeLower.includes('smallint') || typeLower.includes('tinyint')) {
        // Integer column
        const statsResult = await conn.runAndReadAll(`
          SELECT 
            MIN("${colName}") as min_val,
            MAX("${colName}") as max_val,
            MEDIAN("${colName}") as median_val
          FROM data
        `);
        const [min, max, median] = statsResult.getRows()[0];
        colStats.min = typeof min === 'bigint' ? Number(min) : min;
        colStats.max = typeof max === 'bigint' ? Number(max) : max;
        colStats.median = typeof median === 'bigint' ? Number(median) : median;
        
      } else if (typeLower.includes('float') || typeLower.includes('double') || typeLower.includes('decimal') || typeLower.includes('numeric')) {
        // Floating point column
        const statsResult = await conn.runAndReadAll(`
          SELECT 
            MIN("${colName}") as min_val,
            MAX("${colName}") as max_val,
            MEDIAN("${colName}") as median_val,
            AVG("${colName}") as mean_val,
            STDDEV("${colName}") as stddev_val
          FROM data
        `);
        const [min, max, median, mean, stddev] = statsResult.getRows()[0];
        colStats.min = typeof min === 'bigint' ? Number(min) : min;
        colStats.max = typeof max === 'bigint' ? Number(max) : max;
        colStats.median = typeof median === 'bigint' ? Number(median) : median;
        colStats.mean = typeof mean === 'bigint' ? Number(mean) : mean;
        colStats.stddev = typeof stddev === 'bigint' ? Number(stddev) : stddev;
        
      } else if (typeLower.includes('varchar') || typeLower.includes('char') || typeLower.includes('text') || typeLower.includes('string')) {
        // String column
        const statsResult = await conn.runAndReadAll(`
          SELECT 
            COUNT(DISTINCT "${colName}") as unique_count,
            MIN(LENGTH("${colName}")) as min_len,
            MAX(LENGTH("${colName}")) as max_len
          FROM data
        `);
        const [uniqueCount, minLen, maxLen] = statsResult.getRows()[0];
        colStats.uniqueCount = typeof uniqueCount === 'bigint' ? Number(uniqueCount) : uniqueCount;
        colStats.minLength = typeof minLen === 'bigint' ? Number(minLen) : minLen;
        colStats.maxLength = typeof maxLen === 'bigint' ? Number(maxLen) : maxLen;
      }
      
      columns.push(colStats);
    }
    
    return { rowCount, columns };
  } catch (error) {
    return { rowCount: 0, columns: [], error: error.message };
  }
}

function parseMultipartFormData(req) {
  return new Promise((resolve, reject) => {
    const contentType = req.headers['content-type'];
    if (!contentType || !contentType.includes('multipart/form-data')) {
      reject(new Error('Not multipart/form-data'));
      return;
    }
    
    const boundaryMatch = contentType.match(/boundary=([^;\s]+)/);
    if (!boundaryMatch) {
      reject(new Error('No boundary found'));
      return;
    }
    
    const boundary = boundaryMatch[1];
    let chunks = [];
    
    req.on('data', chunk => chunks.push(chunk));
    
    req.on('end', () => {
      try {
        const data = Buffer.concat(chunks);
        const boundaryBuffer = Buffer.from('--' + boundary);
        const endBoundaryBuffer = Buffer.from('--' + boundary + '--');
        
        const parts = [];
        let idx = 0;
        
        while (idx < data.length) {
          // Find next boundary
          let boundaryIdx = data.indexOf(boundaryBuffer, idx);
          if (boundaryIdx === -1) break;
          
          // Move past the boundary
          idx = boundaryIdx + boundaryBuffer.length;
          
          // Check if this is the end boundary
          if (data.slice(idx, idx + 2).toString() === '--') {
            break;
          }
          
          // Skip \r\n after boundary
          if (data.slice(idx, idx + 2).toString() === '\r\n') {
            idx += 2;
          }
          
          // Find next boundary to get the part content
          let nextBoundaryIdx = data.indexOf(boundaryBuffer, idx);
          if (nextBoundaryIdx === -1) break;
          
          // Get the part (excluding the final \r\n before boundary)
          const part = data.slice(idx, nextBoundaryIdx - 2);
          
          // Split headers and content
          const headerEnd = part.indexOf('\r\n\r\n');
          if (headerEnd === -1) continue;
          
          const headers = part.slice(0, headerEnd).toString();
          const content = part.slice(headerEnd + 4);
          
          // Parse Content-Disposition
          const nameMatch = headers.match(/name="([^"]+)"/);
          const filenameMatch = headers.match(/filename="([^"]+)"/);
          
          if (nameMatch) {
            parts.push({
              name: nameMatch[1],
              filename: filenameMatch ? filenameMatch[1] : null,
              headers,
              data: content
            });
          }
        }
        
        resolve(parts);
      } catch (err) {
        reject(err);
      }
    });
    
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  
  if (url.pathname === '/' && req.method === 'GET') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) {
        res.writeHead(500);
        res.end('Error loading index.html');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
    return;
  }
  
  if (url.pathname === '/api/load' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      try {
        const { filePath } = JSON.parse(body);
        const result = await loadFile(filePath);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (error) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: error.message }));
      }
    });
    return;
  }
  
  if (url.pathname === '/api/upload' && req.method === 'POST') {
    try {
      const parts = await parseMultipartFormData(req);
      const filePart = parts.find(p => p.name === 'file' && p.filename);
      
      if (!filePart) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'No file uploaded' }));
        return;
      }
      
      // Save to temp file
      const tempPath = path.join(os.tmpdir(), `duckdb-${Date.now()}-${filePart.filename}`);
      fs.writeFileSync(tempPath, filePart.data);
      
      // Load into DuckDB
      const result = await loadFile(tempPath);
      
      // Clean up temp file
      try {
        fs.unlinkSync(tempPath);
      } catch (e) {
        // Ignore cleanup errors
      }
      
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: error.message }));
    }
    return;
  }
  
  if (url.pathname === '/api/data' && req.method === 'GET') {
    try {
      const whereClause = url.searchParams.get('where');
      const data = await getData(whereClause);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }
  
  if (url.pathname === '/api/summary' && req.method === 'GET') {
    try {
      const summary = await getSummary();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(summary));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }
  
  res.writeHead(404);
  res.end('Not found');
});

const PORT = 3000;
server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
