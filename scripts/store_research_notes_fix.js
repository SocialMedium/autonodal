// Replace the storeResearchNotes function (around line 331) with this:

async function storeResearchNotes(personId, notes, project) {
  for (const note of notes) {
    try {
      const noteText = note.text || note.textStripped;
      
      if (!noteText || noteText.trim() === '') {
        continue; // Skip empty notes
      }
      
      // Check if this exact content already exists for this person
      // Use MD5 hash to compare content regardless of external_id
      const existing = await pool.query(`
        SELECT id FROM interactions 
        WHERE person_id = $1 
          AND interaction_type = 'research_note'
          AND MD5(summary) = MD5($2)
      `, [personId, noteText]);
      
      if (existing.rows.length > 0) {
        continue; // Already have this exact note for this person
      }
      
      // Store as interaction with project context
      await pool.query(`
        INSERT INTO interactions (
          person_id,
          user_id,
          interaction_type,
          source,
          external_id,
          summary,
          metadata,
          created_at,
          interaction_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8)
      `, [
        personId,
        '13ab009a-62b1-4023-80e3-6241cbcda25d', // Your user ID
        'research_note',
        'ezekia',
        String(note.id),
        noteText,
        JSON.stringify({
          project_id: project.id,
          project_name: project.name,
          client_company: project.relationships?.company?.name
        }),
        note.date
      ]);
      
      stats.notes.total++;
      
    } catch (error) {
      console.error(`Error storing note ${note.id}:`, error.message);
    }
  }
}