const express = require('express');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = 3000;

const PRIORITY_VALUES = { HIGH: 0, MEDIUM: 1, LOW: 2 };


const ingestionStore = {};
const batchQueue = [];

function createBatches(ids) {
  const batches = [];
  for (let i = 0; i < ids.length; i += 3) {
    batches.push(ids.slice(i, i + 3));
  }
  return batches;
}

function getIngestionStatus(batches) {
  const statuses = batches.map((b) => b.status);
  if (statuses.every((s) => s === 'yet_to_start')) return 'yet_to_start';
  if (statuses.every((s) => s === 'completed')) return 'completed';
  return 'triggered';
}


function simulateProcessing(id) {
  return new Promise((resolve) =>
    setTimeout(() => resolve({ id, data: 'processed' }), 1000)
  );
}

setInterval(async () => {
  if (batchQueue.length === 0) return;

  batchQueue.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.timestamp - b.timestamp;
  });

  const next = batchQueue.shift();
  const { ingestionId, batch } = next;

  batch.status = 'triggered';

  await Promise.all(batch.ids.map(simulateProcessing));
  batch.status = 'completed';
}, 5000);

app.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;

  if (!Array.isArray(ids) || !priority || !PRIORITY_VALUES.hasOwnProperty(priority)) {
    return res.status(400).json({ error: 'Invalid input' });
  }

  const ingestionId = uuidv4();
  const timestamp = Date.now();
  const batches = createBatches(ids).map((batchIds) => ({
    batch_id: uuidv4(),
    ids: batchIds,
    status: 'yet_to_start',
  }));

  ingestionStore[ingestionId] = {
    ingestion_id: ingestionId,
    priority,
    timestamp,
    batches,
  };

  batches.forEach((batch) => {
    batchQueue.push({
      ingestionId,
      batch,
      priority: PRIORITY_VALUES[priority],
      timestamp,
    });
  });

  res.json({ ingestion_id: ingestionId });
});

app.get('/status/:ingestionId', (req, res) => {
  const { ingestionId } = req.params;
  const ingestion = ingestionStore[ingestionId];
  if (!ingestion) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }

  const status = getIngestionStatus(ingestion.batches);

  res.json({
    ingestion_id: ingestion.ingestion_id,
    status,
    batches: ingestion.batches,
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
