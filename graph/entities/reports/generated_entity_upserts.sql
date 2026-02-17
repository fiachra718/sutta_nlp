-- Generated from graph/entities/reports/entities_to_fix.psv
-- Includes only resolution candidates after note + boundary-junk exclusion.
BEGIN;

-- 1) Canonical inserts
-- GPE | Agga.lava | refs: thag.21.00.irel.html:1263, thag.21.00.irel.html:15
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Agga.lava', lower('Agga.lava'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Agga.lava') OR e.normalized=lower('Agga.lava'))
);

-- GPE | Ambara | refs: dn.32.0.piya.html:40
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Ambara', lower('Ambara'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Ambara') OR e.normalized=lower('Ambara'))
);

-- GPE | Gayasisa | refs: sn35.028.nymo.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Gayasisa', lower('Gayasisa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Gayasisa') OR e.normalized=lower('Gayasisa'))
);

-- GPE | Gutijjita | refs: mn.116.piya.html:22
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Gutijjita', lower('Gutijjita'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Gutijjita') OR e.normalized=lower('Gutijjita'))
);

-- GPE | Jali | refs: mn.116.piya.html:21
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Jali', lower('Jali'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Jali') OR e.normalized=lower('Jali'))
);

-- GPE | Kappa | refs: thag.21.00.irel.html:1272, thag.21.00.irel.html:1273, thag.21.00.irel.html:15
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Kappa', lower('Kappa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Kappa') OR e.normalized=lower('Kappa'))
);

-- GPE | Licchavin | refs: mn.105.than.html:38
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Licchavin', lower('Licchavin'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Licchavin') OR e.normalized=lower('Licchavin'))
);

-- GPE | Oriental | refs: sn56.011.harv.html:21
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Oriental', lower('Oriental'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Oriental') OR e.normalized=lower('Oriental'))
);

-- GPE | Parileyyaka | refs: sn22.081.than.html:4, sn22.081.than.html:6, ud.4.05.irel.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Parileyyaka', lower('Parileyyaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Parileyyaka') OR e.normalized=lower('Parileyyaka'))
);

-- GPE | Passage | refs: khp.1-9.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Passage', lower('Passage'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Passage') OR e.normalized=lower('Passage'))
);

-- GPE | Pavarika | refs: dn.16.1-6.vaji.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Pavarika', lower('Pavarika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Pavarika') OR e.normalized=lower('Pavarika'))
);

-- GPE | Rahula | refs: mn.062.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Rahula', lower('Rahula'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Rahula') OR e.normalized=lower('Rahula'))
);

-- GPE | Rama | refs: mn.026.than.html:34, mn.036.than.html:32
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Rama', lower('Rama'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Rama') OR e.normalized=lower('Rama'))
);

-- GPE | Rosika | refs: dn.12.0.than.html:3, dn.12.0.than.html:4, dn.12.0.than.html:6, dn.12.0.than.html:7, dn.12.0.than.html:8, dn.12.0.than.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Rosika', lower('Rosika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Rosika') OR e.normalized=lower('Rosika'))
);

-- GPE | Sacca | refs: mn.116.piya.html:21
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Sacca', lower('Sacca'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Sacca') OR e.normalized=lower('Sacca'))
);

-- GPE | Savatthia | refs: sn44.001.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Savatthia', lower('Savatthia'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Savatthia') OR e.normalized=lower('Savatthia'))
);

-- GPE | Setabya | refs: an04.036.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Setabya', lower('Setabya'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Setabya') OR e.normalized=lower('Setabya'))
);

-- GPE | Sindh | refs: dhp.23.than.html:322
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Sindh', lower('Sindh'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Sindh') OR e.normalized=lower('Sindh'))
);

-- GPE | Skill | refs: mn.035.than.html:80, mn.136.than.html:39
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Skill', lower('Skill'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Skill') OR e.normalized=lower('Skill'))
);

-- GPE | Sunaparanta | refs: sn35.088.than.html:17, sn35.088.than.html:18, sn35.088.than.html:19, sn35.088.than.html:21, sn35.088.than.html:23, sn35.088.than.html:25, sn35.088.than.html:27
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Sunaparanta', lower('Sunaparanta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Sunaparanta') OR e.normalized=lower('Sunaparanta'))
);

-- GPE | Suttanta | refs: mn.008.nypo.html:8
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Suttanta', lower('Suttanta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Suttanta') OR e.normalized=lower('Suttanta'))
);

-- GPE | Theravada | refs: khp.1-9.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Theravada', lower('Theravada'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Theravada') OR e.normalized=lower('Theravada'))
);

-- GPE | Tusita | refs: snp.4.16.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Tusita', lower('Tusita'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Tusita') OR e.normalized=lower('Tusita'))
);

-- GPE | Vebhara | refs: thag.01.00x.than.html:41
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Vebhara', lower('Vebhara'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Vebhara') OR e.normalized=lower('Vebhara'))
);

-- GPE | Vethadipa | refs: dn.16.1-6.vaji.html:42
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Vethadipa', lower('Vethadipa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Vethadipa') OR e.normalized=lower('Vethadipa'))
);

-- GPE | Yana | refs: mn.093.than.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'GPE', 'Yana', lower('Yana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'GPE'
    AND (lower(e.canonical)=lower('Yana') OR e.normalized=lower('Yana'))
);

-- LOC | Abhassara | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Abhassara', lower('Abhassara'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Abhassara') OR e.normalized=lower('Abhassara'))
);

-- LOC | Akanittha | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Akanittha', lower('Akanittha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Akanittha') OR e.normalized=lower('Akanittha'))
);

-- LOC | Ambāṭakavana | refs: sn41.005.niza.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Ambāṭakavana', lower('Ambāṭakavana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Ambāṭakavana') OR e.normalized=lower('Ambāṭakavana'))
);

-- LOC | Appamana subha | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Appamana subha', lower('Appamana subha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Appamana subha') OR e.normalized=lower('Appamana subha'))
);

-- LOC | Appamanabha | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Appamanabha', lower('Appamanabha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Appamanabha') OR e.normalized=lower('Appamanabha'))
);

-- LOC | Ariyan discipline | refs: sn35.187.wlsh.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Ariyan discipline', lower('Ariyan discipline'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Ariyan discipline') OR e.normalized=lower('Ariyan discipline'))
);

-- LOC | Atappa | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Atappa', lower('Atappa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Atappa') OR e.normalized=lower('Atappa'))
);

-- LOC | Aviha | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Aviha', lower('Aviha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Aviha') OR e.normalized=lower('Aviha'))
);

-- LOC | Bahumati | refs: mn.007.nypo.html:12
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Bahumati', lower('Bahumati'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Bahumati') OR e.normalized=lower('Bahumati'))
);

-- LOC | Balava-abyss | refs: thag.19.00.khan.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Balava-abyss', lower('Balava-abyss'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Balava-abyss') OR e.normalized=lower('Balava-abyss'))
);

-- LOC | Bhagalavati | refs: dn.32.0.piya.html:40
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Bhagalavati', lower('Bhagalavati'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Bhagalavati') OR e.normalized=lower('Bhagalavati'))
);

-- LOC | Black Forest | refs: an09.037.than.html:10
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Black Forest', lower('Black Forest'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Black Forest') OR e.normalized=lower('Black Forest'))
);

-- LOC | Boar's Cave | refs: mn.074.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Boar''s Cave', lower('Boar''s Cave'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Boar''s Cave') OR e.normalized=lower('Boar''s Cave'))
);

-- LOC | Bodhi Tree | refs: ud.1.01.irel.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Bodhi Tree', lower('Bodhi Tree'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Bodhi Tree') OR e.normalized=lower('Bodhi Tree'))
);

-- LOC | Brahma | refs: an08.041.vaka.html:5, dn.11.0.than.html:117, dn.11.0.than.html:125, dn.16.5-6.than.html:76, dn.16.5-6.than.html:77, dn.20.0.than.html:90, mn.049.than.html:21, mn.049.than.html:22, mn.049.than.html:24, mn.049.than.html:25, mn.049.than.html:27, mn.049.than.html:32, mn.049.than.html:33, mn.049.than.html:41
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Brahma', lower('Brahma'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Brahma') OR e.normalized=lower('Brahma'))
);

-- LOC | Brahma Parisajja | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Brahma Parisajja', lower('Brahma Parisajja'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Brahma Parisajja') OR e.normalized=lower('Brahma Parisajja'))
);

-- LOC | Brahma Purohita | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Brahma Purohita', lower('Brahma Purohita'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Brahma Purohita') OR e.normalized=lower('Brahma Purohita'))
);

-- LOC | Brahma-Uposatha | refs: an03.070.than.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Brahma-Uposatha', lower('Brahma-Uposatha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Brahma-Uposatha') OR e.normalized=lower('Brahma-Uposatha'))
);

-- LOC | Brahma-faring | refs: an08.041.vaka.html:4, mn.125.horn.html:34
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Brahma-faring', lower('Brahma-faring'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Brahma-faring') OR e.normalized=lower('Brahma-faring'))
);

-- LOC | Brahmā he | refs: ud.7.06.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Brahmā he', lower('Brahmā he'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Brahmā he') OR e.normalized=lower('Brahmā he'))
);

-- LOC | Brick House | refs: dn.16.1-6.vaji.html:11, dn.16.1-6.vaji.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Brick House', lower('Brick House'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Brick House') OR e.normalized=lower('Brick House'))
);

-- LOC | Buddha-sasana | refs: thag.19.00.khan.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Buddha-sasana', lower('Buddha-sasana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Buddha-sasana') OR e.normalized=lower('Buddha-sasana'))
);

-- LOC | Buffalo Ground | refs: an08.008.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Buffalo Ground', lower('Buffalo Ground'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Buffalo Ground') OR e.normalized=lower('Buffalo Ground'))
);

-- LOC | Calika Hill | refs: ud.4.01.irel.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Calika Hill', lower('Calika Hill'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Calika Hill') OR e.normalized=lower('Calika Hill'))
);

-- LOC | Catummaharajika | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Catummaharajika', lower('Catummaharajika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Catummaharajika') OR e.normalized=lower('Catummaharajika'))
);

-- LOC | Cool Grove | refs: sn10.008.than.html:10, sn10.008.than.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Cool Grove', lower('Cool Grove'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Cool Grove') OR e.normalized=lower('Cool Grove'))
);

-- LOC | Dakkhinagiri | refs: sn07.011.piya.html:3, sn07.011.than.html:4, snp.1.04.piya.html:22, snp.1.04.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Dakkhinagiri', lower('Dakkhinagiri'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Dakkhinagiri') OR e.normalized=lower('Dakkhinagiri'))
);

-- LOC | Deer Park at Bhesakala Grove | refs: an04.055.than.html:2, an06.016.than.html:1, an06.016.than.html:5, an06.016.than.html:6, an06.016.than.html:7, an07.058.than.html:1, an08.030.than.html:10, an08.030.than.html:2, an08.030.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Deer Park at Bhesakala Grove', lower('Deer Park at Bhesakala Grove'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Deer Park at Bhesakala Grove') OR e.normalized=lower('Deer Park at Bhesakala Grove'))
);

-- LOC | Deer Park in Isipatana | refs: mn.026.than.html:57
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Deer Park in Isipatana', lower('Deer Park in Isipatana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Deer Park in Isipatana') OR e.normalized=lower('Deer Park in Isipatana'))
);

-- LOC | Eastern Park | refs: mn.026.than.html:10, mn.026.than.html:8
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Eastern Park', lower('Eastern Park'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Eastern Park') OR e.normalized=lower('Eastern Park'))
);

-- LOC | Eastern Park at Migara's mother | refs: ud.5.05.irel.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Eastern Park at Migara''s mother', lower('Eastern Park at Migara''s mother'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Eastern Park at Migara''s mother') OR e.normalized=lower('Eastern Park at Migara''s mother'))
);

-- LOC | Forest Hut | refs: mn.125.horn.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Forest Hut', lower('Forest Hut'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Forest Hut') OR e.normalized=lower('Forest Hut'))
);

-- LOC | Game Refuge | refs: an09.037.than.html:10
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Game Refuge', lower('Game Refuge'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Game Refuge') OR e.normalized=lower('Game Refuge'))
);

-- LOC | Giribbaja's | refs: thag.19.00.khan.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Giribbaja''s', lower('Giribbaja''s'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Giribbaja''s') OR e.normalized=lower('Giribbaja''s'))
);

-- LOC | Giribbaja's wilds | refs: thag.19.00.khan.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Giribbaja''s wilds', lower('Giribbaja''s wilds'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Giribbaja''s wilds') OR e.normalized=lower('Giribbaja''s wilds'))
);

-- LOC | Gotama Ford | refs: ud.8.06.than.html:33
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Gotama Ford', lower('Gotama Ford'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Gotama Ford') OR e.normalized=lower('Gotama Ford'))
);

-- LOC | Gotama Gate | refs: ud.8.06.than.html:33, ud.8.06.than.html:34
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Gotama Gate', lower('Gotama Gate'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Gotama Gate') OR e.normalized=lower('Gotama Gate'))
);

-- LOC | Gotama-ford | refs: dn.16.1-6.vaji.html:32
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Gotama-ford', lower('Gotama-ford'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Gotama-ford') OR e.normalized=lower('Gotama-ford'))
);

-- LOC | Great Roruva hell | refs: sn03.020.than.html:7, sn03.020.than.html:8, sn03.020.than.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Great Roruva hell', lower('Great Roruva hell'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Great Roruva hell') OR e.normalized=lower('Great Roruva hell'))
);

-- LOC | Group of Thirty | refs: snp.3.11.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Group of Thirty', lower('Group of Thirty'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Group of Thirty') OR e.normalized=lower('Group of Thirty'))
);

-- LOC | Gunda Forest | refs: an02.038.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Gunda Forest', lower('Gunda Forest'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Gunda Forest') OR e.normalized=lower('Gunda Forest'))
);

-- LOC | High Divinity's retinue | refs: an04.125.nymo.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'High Divinity''s retinue', lower('High Divinity''s retinue'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('High Divinity''s retinue') OR e.normalized=lower('High Divinity''s retinue'))
);

-- LOC | Hiraññavati | refs: dn.16.1-6.vaji.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Hiraññavati', lower('Hiraññavati'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Hiraññavati') OR e.normalized=lower('Hiraññavati'))
);

-- LOC | Inspiration Peak | refs: sn56.042.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Inspiration Peak', lower('Inspiration Peak'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Inspiration Peak') OR e.normalized=lower('Inspiration Peak'))
);

-- LOC | Isipatana | refs: mn.141.piya.html:39, mn.141.piya.html:8, sn22.059.mend.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Isipatana', lower('Isipatana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Isipatana') OR e.normalized=lower('Isipatana'))
);

-- LOC | Jeta Forest | refs: sn12.061.niza.html:1, ud.7.05.niza.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Jeta Forest', lower('Jeta Forest'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Jeta Forest') OR e.normalized=lower('Jeta Forest'))
);

-- LOC | Jivaka's | refs: thig.14.01.than.html:147
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Jivaka''s', lower('Jivaka''s'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Jivaka''s') OR e.normalized=lower('Jivaka''s'))
);

-- LOC | Jujube Tree Park | refs: sn22.089.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Jujube Tree Park', lower('Jujube Tree Park'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Jujube Tree Park') OR e.normalized=lower('Jujube Tree Park'))
);

-- LOC | Kukkata Monastery | refs: an11.017.than.html:1, mn.052.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Kukkata Monastery', lower('Kukkata Monastery'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Kukkata Monastery') OR e.normalized=lower('Kukkata Monastery'))
);

-- LOC | Licchavi-son | refs: mn.035.than.html:60, mn.035.than.html:63
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Licchavi-son', lower('Licchavi-son'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Licchavi-son') OR e.normalized=lower('Licchavi-son'))
);

-- LOC | Maddakucchi Deer | refs: sn01.038.than.html:2, sn04.013.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Maddakucchi Deer', lower('Maddakucchi Deer'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Maddakucchi Deer') OR e.normalized=lower('Maddakucchi Deer'))
);

-- LOC | Mahisavatthu | refs: an08.008.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mahisavatthu', lower('Mahisavatthu'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mahisavatthu') OR e.normalized=lower('Mahisavatthu'))
);

-- LOC | Makuta-bandhana | refs: dn.16.1-6.vaji.html:23, dn.16.1-6.vaji.html:25, dn.16.1-6.vaji.html:30, dn.16.5-6.than.html:112, dn.16.5-6.than.html:114, dn.16.5-6.than.html:131
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Makuta-bandhana', lower('Makuta-bandhana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Makuta-bandhana') OR e.normalized=lower('Makuta-bandhana'))
);

-- LOC | Mango Grove | refs: dn.16.1-6.vaji.html:53
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mango Grove', lower('Mango Grove'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mango Grove') OR e.normalized=lower('Mango Grove'))
);

-- LOC | Mango Orchard | refs: ud.2.10.irel.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mango Orchard', lower('Mango Orchard'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mango Orchard') OR e.normalized=lower('Mango Orchard'))
);

-- LOC | Mango Stone | refs: mn.061.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mango Stone', lower('Mango Stone'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mango Stone') OR e.normalized=lower('Mango Stone'))
);

-- LOC | Matanga wilds | refs: dhp.23.than.html:329, dhp.23.than.html:330
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Matanga wilds', lower('Matanga wilds'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Matanga wilds') OR e.normalized=lower('Matanga wilds'))
);

-- LOC | Migacira pleasure garden | refs: mn.082.than.html:62
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Migacira pleasure garden', lower('Migacira pleasure garden'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Migacira pleasure garden') OR e.normalized=lower('Migacira pleasure garden'))
);

-- LOC | Migara 's mother | refs: an08.043.khan.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Migara ''s mother', lower('Migara ''s mother'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Migara ''s mother') OR e.normalized=lower('Migara ''s mother'))
);

-- LOC | Mount Meru's summit | refs: snp.3.11.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mount Meru''s summit', lower('Mount Meru''s summit'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mount Meru''s summit') OR e.normalized=lower('Mount Meru''s summit'))
);

-- LOC | Mount Pandava | refs: snp.3.01.than.html:7, snp.3.01.than.html:8
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mount Pandava', lower('Mount Pandava'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mount Pandava') OR e.normalized=lower('Mount Pandava'))
);

-- LOC | Mount Sata | refs: dn.20.0.than.html:8
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mount Sata', lower('Mount Sata'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mount Sata') OR e.normalized=lower('Mount Sata'))
);

-- LOC | Mount Sineru | refs: thig.14.01.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mount Sineru', lower('Mount Sineru'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mount Sineru') OR e.normalized=lower('Mount Sineru'))
);

-- LOC | Mount Vulture Peak | refs: thig.05.09.hekh.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mount Vulture Peak', lower('Mount Vulture Peak'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mount Vulture Peak') OR e.normalized=lower('Mount Vulture Peak'))
);

-- LOC | Mucalinda Tree | refs: ud.2.01.irel.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mucalinda Tree', lower('Mucalinda Tree'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mucalinda Tree') OR e.normalized=lower('Mucalinda Tree'))
);

-- LOC | Mulapariyaya Sutta | refs: an03.123.than.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Mulapariyaya Sutta', lower('Mulapariyaya Sutta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Mulapariyaya Sutta') OR e.normalized=lower('Mulapariyaya Sutta'))
);

-- LOC | Nigrodha Park | refs: an08.025.kuma.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Nigrodha Park', lower('Nigrodha Park'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Nigrodha Park') OR e.normalized=lower('Nigrodha Park'))
);

-- LOC | Nimmanarati devas | refs: an08.043.khan.html:26
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Nimmanarati devas', lower('Nimmanarati devas'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Nimmanarati devas') OR e.normalized=lower('Nimmanarati devas'))
);

-- LOC | Nimmanarati gods | refs: dn.11.0.than.html:109
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Nimmanarati gods', lower('Nimmanarati gods'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Nimmanarati gods') OR e.normalized=lower('Nimmanarati gods'))
);

-- LOC | Niraya -hell | refs: snp.2.06.irel.html:1, snp.2.10.irel.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Niraya -hell', lower('Niraya -hell'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Niraya -hell') OR e.normalized=lower('Niraya -hell'))
);

-- LOC | Niraya hell | refs: mn.125.horn.html:33
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Niraya hell', lower('Niraya hell'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Niraya hell') OR e.normalized=lower('Niraya hell'))
);

-- LOC | Northern clime | refs: dn.20.0.piya.html:25
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Northern clime', lower('Northern clime'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Northern clime') OR e.normalized=lower('Northern clime'))
);

-- LOC | Osprey's Haunt | refs: sn22.003.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Osprey''s Haunt', lower('Osprey''s Haunt'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Osprey''s Haunt') OR e.normalized=lower('Osprey''s Haunt'))
);

-- LOC | Paranimmita-vasavatti | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Paranimmita-vasavatti', lower('Paranimmita-vasavatti'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Paranimmita-vasavatti') OR e.normalized=lower('Paranimmita-vasavatti'))
);

-- LOC | Paranimmita-vasavatti devas | refs: sn56.011.than.html:14
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Paranimmita-vasavatti devas', lower('Paranimmita-vasavatti devas'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Paranimmita-vasavatti devas') OR e.normalized=lower('Paranimmita-vasavatti devas'))
);

-- LOC | Paranimmitavasavatti | refs: dn.11.0.than.html:111, dn.11.0.than.html:112
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Paranimmitavasavatti', lower('Paranimmitavasavatti'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Paranimmitavasavatti') OR e.normalized=lower('Paranimmitavasavatti'))
);

-- LOC | Parittabha | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Parittabha', lower('Parittabha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Parittabha') OR e.normalized=lower('Parittabha'))
);

-- LOC | Parittasubha | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Parittasubha', lower('Parittasubha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Parittasubha') OR e.normalized=lower('Parittasubha'))
);

-- LOC | Payaga | refs: mn.007.nypo.html:12
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Payaga', lower('Payaga'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Payaga') OR e.normalized=lower('Payaga'))
);

-- LOC | Peaked Pavilion | refs: mn.105.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Peaked Pavilion', lower('Peaked Pavilion'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Peaked Pavilion') OR e.normalized=lower('Peaked Pavilion'))
);

-- LOC | Peaked Roof Hall | refs: an08.053.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Peaked Roof Hall', lower('Peaked Roof Hall'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Peaked Roof Hall') OR e.normalized=lower('Peaked Roof Hall'))
);

-- LOC | Pepper Tree Cave | refs: sn46.014.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Pepper Tree Cave', lower('Pepper Tree Cave'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Pepper Tree Cave') OR e.normalized=lower('Pepper Tree Cave'))
);

-- LOC | Pigeon Cave | refs: ud.4.04.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Pigeon Cave', lower('Pigeon Cave'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Pigeon Cave') OR e.normalized=lower('Pigeon Cave'))
);

-- LOC | Piyadassi | refs: khp.1-9.than.html:6, mn.116.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Piyadassi', lower('Piyadassi'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Piyadassi') OR e.normalized=lower('Piyadassi'))
);

-- LOC | Plum-Mango Grove | refs: sn41.005.niza.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Plum-Mango Grove', lower('Plum-Mango Grove'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Plum-Mango Grove') OR e.normalized=lower('Plum-Mango Grove'))
);

-- LOC | Realm of the Thirty-three | refs: mn.041.nymo.html:19
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Realm of the Thirty-three', lower('Realm of the Thirty-three'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Realm of the Thirty-three') OR e.normalized=lower('Realm of the Thirty-three'))
);

-- LOC | Retinue of High Divinity | refs: sn56.011.nymo.html:16
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Retinue of High Divinity', lower('Retinue of High Divinity'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Retinue of High Divinity') OR e.normalized=lower('Retinue of High Divinity'))
);

-- LOC | River Yamuna | refs: dn.20.0.than.html:38
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'River Yamuna', lower('River Yamuna'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('River Yamuna') OR e.normalized=lower('River Yamuna'))
);

-- LOC | Sal Tree | refs: sn22.081.than.html:4, sn22.081.than.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sal Tree', lower('Sal Tree'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sal Tree') OR e.normalized=lower('Sal Tree'))
);

-- LOC | Sal Tree Grove | refs: sn06.015.than.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sal Tree Grove', lower('Sal Tree Grove'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sal Tree Grove') OR e.normalized=lower('Sal Tree Grove'))
);

-- LOC | Sarabhu | refs: an10.015.than.html:10, ud.5.05.irel.html:4, ud.5.05.than.html:14, ud.5.05.than.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sarabhu', lower('Sarabhu'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sarabhu') OR e.normalized=lower('Sarabhu'))
);

-- LOC | Sarasvati | refs: thag.19.00.khan.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sarasvati', lower('Sarasvati'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sarasvati') OR e.normalized=lower('Sarasvati'))
);

-- LOC | Sayha | refs: mn.116.piya.html:25
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sayha', lower('Sayha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sayha') OR e.normalized=lower('Sayha'))
);

-- LOC | Setabya | refs: an04.036.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Setabya', lower('Setabya'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Setabya') OR e.normalized=lower('Setabya'))
);

-- LOC | Sheer-face Peak | refs: sn22.003.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sheer-face Peak', lower('Sheer-face Peak'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sheer-face Peak') OR e.normalized=lower('Sheer-face Peak'))
);

-- LOC | Simbali Forest | refs: mn.130.than.html:56
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Simbali Forest', lower('Simbali Forest'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Simbali Forest') OR e.normalized=lower('Simbali Forest'))
);

-- LOC | Squirrels' refuge | refs: ud.3.06.than.html:2, ud.5.08.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Squirrels'' refuge', lower('Squirrels'' refuge'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Squirrels'' refuge') OR e.normalized=lower('Squirrels'' refuge'))
);

-- LOC | Subhakinna | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Subhakinna', lower('Subhakinna'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Subhakinna') OR e.normalized=lower('Subhakinna'))
);

-- LOC | Sudassa | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sudassa', lower('Sudassa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sudassa') OR e.normalized=lower('Sudassa'))
);

-- LOC | Sudassana | refs: mn.116.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sudassana', lower('Sudassana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sudassana') OR e.normalized=lower('Sudassana'))
);

-- LOC | Sudassi | refs: sn56.011.piya.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sudassi', lower('Sudassi'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sudassi') OR e.normalized=lower('Sudassi'))
);

-- LOC | Sundarika | refs: mn.007.nypo.html:12
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sundarika', lower('Sundarika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sundarika') OR e.normalized=lower('Sundarika'))
);

-- LOC | Super Manusas | refs: dn.20.0.than.html:70
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Super Manusas', lower('Super Manusas'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Super Manusas') OR e.normalized=lower('Super Manusas'))
);

-- LOC | Sword-leaf Forest | refs: mn.130.than.html:56, mn.130.than.html:57
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sword-leaf Forest', lower('Sword-leaf Forest'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sword-leaf Forest') OR e.normalized=lower('Sword-leaf Forest'))
);

-- LOC | Sāmaṇḍakāni | refs: an10.065.niza.html:1, an10.066.niza.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Sāmaṇḍakāni', lower('Sāmaṇḍakāni'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Sāmaṇḍakāni') OR e.normalized=lower('Sāmaṇḍakāni'))
);

-- LOC | Tandulapala Gate | refs: mn.097.than.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Tandulapala Gate', lower('Tandulapala Gate'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Tandulapala Gate') OR e.normalized=lower('Tandulapala Gate'))
);

-- LOC | Tusita | refs: dn.20.0.piya.html:56, sn56.011.piya.html:17, ud.5.02.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'Tusita', lower('Tusita'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('Tusita') OR e.normalized=lower('Tusita'))
);

-- LOC | palace of Brahmā | refs: dn.01.0.bodh.html:41
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'palace of Brahmā', lower('palace of Brahmā'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('palace of Brahmā') OR e.normalized=lower('palace of Brahmā'))
);

-- LOC | river Hiraññavati | refs: dn.16.1-6.vaji.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'river Hiraññavati', lower('river Hiraññavati'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('river Hiraññavati') OR e.normalized=lower('river Hiraññavati'))
);

-- LOC | river Kimikala | refs: ud.4.01.irel.html:4, ud.4.01.irel.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'LOC', 'river Kimikala', lower('river Kimikala'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'LOC'
    AND (lower(e.canonical)=lower('river Kimikala') OR e.normalized=lower('river Kimikala'))
);

-- PERSON | Accuta | refs: dn.20.0.piya.html:48, mn.116.piya.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Accuta', lower('Accuta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Accuta') OR e.normalized=lower('Accuta'))
);

-- PERSON | Aggikabharadvaja | refs: snp.1.07.piya.html:2, snp.1.07.piya.html:28
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Aggikabharadvaja', lower('Aggikabharadvaja'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Aggikabharadvaja') OR e.normalized=lower('Aggikabharadvaja'))
);

-- PERSON | Ajakalāpaka | refs: ud.1.07.than.html:2, ud.1.07.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Ajakalāpaka', lower('Ajakalāpaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Ajakalāpaka') OR e.normalized=lower('Ajakalāpaka'))
);

-- PERSON | Amaravati | refs: khp.1-9.than.html:10, khp.1-9x.piya.html:10
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Amaravati', lower('Amaravati'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Amaravati') OR e.normalized=lower('Amaravati'))
);

-- PERSON | Araka | refs: an07.070.than.html:1, an07.070.than.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Araka', lower('Araka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Araka') OR e.normalized=lower('Araka'))
);

-- PERSON | Asama | refs: dn.20.0.piya.html:43, dn.20.0.than.html:56
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Asama', lower('Asama'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Asama') OR e.normalized=lower('Asama'))
);

-- PERSON | Atula | refs: dhp.17.budd.html:1, dhp.17.budd.html:227, dhp.17.than.html:227
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Atula', lower('Atula'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Atula') OR e.normalized=lower('Atula'))
);

-- PERSON | Añña-Kondañña | refs: sn56.011.nymo.html:17, sn56.011.than.html:16
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Añña-Kondañña', lower('Añña-Kondañña'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Añña-Kondañña') OR e.normalized=lower('Añña-Kondañña'))
);

-- PERSON | Bali | refs: dn.20.0.piya.html:39, dn.20.0.than.html:50
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Bali', lower('Bali'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Bali') OR e.normalized=lower('Bali'))
);

-- PERSON | Bhaaradvaaja Brahman | refs: sn07.001.wlsh.html:3, sn07.001.wlsh.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Bhaaradvaaja Brahman', lower('Bhaaradvaaja Brahman'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Bhaaradvaaja Brahman') OR e.normalized=lower('Bhaaradvaaja Brahman'))
);

-- PERSON | Bhadravudha | refs: snp.5.12.than.html:1, snp.5.12.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Bhadravudha', lower('Bhadravudha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Bhadravudha') OR e.normalized=lower('Bhadravudha'))
);

-- PERSON | Bhante | refs: an04.067.piya.html:4, an10.060.piya.html:4, dn.32.0.piya.html:32, mn.116.piya.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Bhante', lower('Bhante'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Bhante') OR e.normalized=lower('Bhante'))
);

-- PERSON | Bhavitatta | refs: mn.116.piya.html:17, mn.116.piya.html:19
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Bhavitatta', lower('Bhavitatta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Bhavitatta') OR e.normalized=lower('Bhavitatta'))
);

-- PERSON | Bound | refs: thag.21.00x.hekh.html:1, ud.7.10.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Bound', lower('Bound'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Bound') OR e.normalized=lower('Bound'))
);

-- PERSON | Buddharakkhita | refs: khp.1-9.than.html:10, khp.1-9x.piya.html:10
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Buddharakkhita', lower('Buddharakkhita'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Buddharakkhita') OR e.normalized=lower('Buddharakkhita'))
);

-- PERSON | Candana | refs: dn.20.0.piya.html:30, dn.20.0.than.html:22, dn.32.0.piya.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Candana', lower('Candana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Candana') OR e.normalized=lower('Candana'))
);

-- PERSON | Carpenter Fivetools | refs: mn.059.nypo.html:1, mn.059.nypo.html:6, sn36.019.nypo.html:1, sn36.019.nypo.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Carpenter Fivetools', lower('Carpenter Fivetools'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Carpenter Fivetools') OR e.normalized=lower('Carpenter Fivetools'))
);

-- PERSON | Citra | refs: dn.20.0.piya.html:35, dn.20.0.than.html:41
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Citra', lower('Citra'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Citra') OR e.normalized=lower('Citra'))
);

-- PERSON | Cittasena | refs: dn.20.0.than.html:27, dn.32.0.piya.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Cittasena', lower('Cittasena'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Cittasena') OR e.normalized=lower('Cittasena'))
);

-- PERSON | Dandapani | refs: mn.018.than.html:2, mn.018.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Dandapani', lower('Dandapani'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Dandapani') OR e.normalized=lower('Dandapani'))
);

-- PERSON | Dhammika | refs: snp.2.14.irel.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Dhammika', lower('Dhammika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Dhammika') OR e.normalized=lower('Dhammika'))
);

-- PERSON | Dhavajalika | refs: an08.008.than.html:1, an08.008.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Dhavajalika', lower('Dhavajalika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Dhavajalika') OR e.normalized=lower('Dhavajalika'))
);

-- PERSON | Elder Kondañña | refs: thag.21.00.irel.html:10, thag.21.00.irel.html:1246
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Elder Kondañña', lower('Elder Kondañña'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Elder Kondañña') OR e.normalized=lower('Elder Kondañña'))
);

-- PERSON | Eraka | refs: thag.01.00x.than.html:93
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Eraka', lower('Eraka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Eraka') OR e.normalized=lower('Eraka'))
);

-- PERSON | Fully Enlightened Ones | refs: dn.16.1-6.vaji.html:17, dn.16.1-6.vaji.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Fully Enlightened Ones', lower('Fully Enlightened Ones'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Fully Enlightened Ones') OR e.normalized=lower('Fully Enlightened Ones'))
);

-- PERSON | Gandhabhaka | refs: sn42.011.than.html:1, sn42.011.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Gandhabhaka', lower('Gandhabhaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Gandhabhaka') OR e.normalized=lower('Gandhabhaka'))
);

-- PERSON | Ghata | refs: mn.122.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Ghata', lower('Ghata'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Ghata') OR e.normalized=lower('Ghata'))
);

-- PERSON | Hatthaka | refs: an08.024.than.html:1, an08.024.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Hatthaka', lower('Hatthaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Hatthaka') OR e.normalized=lower('Hatthaka'))
);

-- PERSON | Hemaka | refs: snp.5.08.than.html:1, snp.5.08.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Hemaka', lower('Hemaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Hemaka') OR e.normalized=lower('Hemaka'))
);

-- PERSON | Jata Bharadvaja | refs: sn07.006.than.html:5, sn07.006.than.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Jata Bharadvaja', lower('Jata Bharadvaja'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Jata Bharadvaja') OR e.normalized=lower('Jata Bharadvaja'))
);

-- PERSON | Kakudha | refs: dn.16.1-6.vaji.html:11, dn.16.1-6.vaji.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kakudha', lower('Kakudha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kakudha') OR e.normalized=lower('Kakudha'))
);

-- PERSON | Kala-khemaka | refs: mn.122.than.html:1, mn.122.than.html:2, mn.122.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kala-khemaka', lower('Kala-khemaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kala-khemaka') OR e.normalized=lower('Kala-khemaka'))
);

-- PERSON | Kalama | refs: mn.026.than.html:22, mn.026.than.html:26, mn.036.than.html:20, mn.036.than.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kalama', lower('Kalama'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kalama') OR e.normalized=lower('Kalama'))
);

-- PERSON | Kaligodha | refs: ud.2.10.irel.html:2, ud.2.10.irel.html:3, ud.2.10.irel.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kaligodha', lower('Kaligodha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kaligodha') OR e.normalized=lower('Kaligodha'))
);

-- PERSON | Kamasettha | refs: dn.20.0.piya.html:30, dn.32.0.piya.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kamasettha', lower('Kamasettha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kamasettha') OR e.normalized=lower('Kamasettha'))
);

-- PERSON | Kandarayana | refs: an02.038.than.html:1, an02.038.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kandarayana', lower('Kandarayana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kandarayana') OR e.normalized=lower('Kandarayana'))
);

-- PERSON | Kanha | refs: dn.20.0.piya.html:64, mn.116.piya.html:28
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kanha', lower('Kanha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kanha') OR e.normalized=lower('Kanha'))
);

-- PERSON | Kappayana | refs: thag.21.00.irel.html:1274, thag.21.00.irel.html:15
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kappayana', lower('Kappayana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kappayana') OR e.normalized=lower('Kappayana'))
);

-- PERSON | Katissabha | refs: dn.16.1-6.vaji.html:12, dn.16.1-6.vaji.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Katissabha', lower('Katissabha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Katissabha') OR e.normalized=lower('Katissabha'))
);

-- PERSON | Kinnughandu | refs: dn.20.0.piya.html:30, dn.20.0.than.html:23, dn.32.0.piya.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kinnughandu', lower('Kinnughandu'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kinnughandu') OR e.normalized=lower('Kinnughandu'))
);

-- PERSON | Kitagiri | refs: mn.070.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kitagiri', lower('Kitagiri'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kitagiri') OR e.normalized=lower('Kitagiri'))
);

-- PERSON | Koravya | refs: an06.054.olen.html:4, an06.054.olen.html:7
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Koravya', lower('Koravya'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Koravya') OR e.normalized=lower('Koravya'))
);

-- PERSON | Kumbhira | refs: dn.20.0.piya.html:18, dn.20.0.than.html:10, dn.20.0.than.html:12
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kumbhira', lower('Kumbhira'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kumbhira') OR e.normalized=lower('Kumbhira'))
);

-- PERSON | Kutendu | refs: dn.20.0.piya.html:29, dn.20.0.than.html:18
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Kutendu', lower('Kutendu'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Kutendu') OR e.normalized=lower('Kutendu'))
);

-- PERSON | Lady Vedehika | refs: mn.021x.than.html:15, mn.021x.than.html:3, mn.021x.than.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Lady Vedehika', lower('Lady Vedehika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Lady Vedehika') OR e.normalized=lower('Lady Vedehika'))
);

-- PERSON | LongNails | refs: mn.074.than.html:1, mn.074.than.html:17
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'LongNails', lower('LongNails'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('LongNails') OR e.normalized=lower('LongNails'))
);

-- PERSON | Mahali | refs: sn22.060.than.html:1, sn22.060.than.html:10, sn22.060.than.html:2, sn22.060.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Mahali', lower('Mahali'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Mahali') OR e.normalized=lower('Mahali'))
);

-- PERSON | Mantani | refs: mn.024.than.html:3, mn.086.than.html:31
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Mantani', lower('Mantani'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Mantani') OR e.normalized=lower('Mantani'))
);

-- PERSON | Master Vacchayana | refs: mn.027.than.html:1, mn.027.than.html:5, mn.027.than.html:7
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Master Vacchayana', lower('Master Vacchayana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Master Vacchayana') OR e.normalized=lower('Master Vacchayana'))
);

-- PERSON | Matanga | refs: mn.116.piya.html:24, snp.1.03.than.html:4, snp.1.07.piya.html:22, snp.1.07.piya.html:23
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Matanga', lower('Matanga'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Matanga') OR e.normalized=lower('Matanga'))
);

-- PERSON | Maya | refs: dn.20.0.than.html:17, thig.06.06.olen.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Maya', lower('Maya'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Maya') OR e.normalized=lower('Maya'))
);

-- PERSON | Mundika | refs: mn.078.than.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Mundika', lower('Mundika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Mundika') OR e.normalized=lower('Mundika'))
);

-- PERSON | Nighandu | refs: dn.20.0.piya.html:30, dn.20.0.than.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Nighandu', lower('Nighandu'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Nighandu') OR e.normalized=lower('Nighandu'))
);

-- PERSON | Nigrodhakappa | refs: thag.21.00.irel.html:1264, thag.21.00.irel.html:15
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Nigrodhakappa', lower('Nigrodhakappa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Nigrodhakappa') OR e.normalized=lower('Nigrodhakappa'))
);

-- PERSON | Nikata | refs: dn.16.1-6.vaji.html:12, dn.16.1-6.vaji.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Nikata', lower('Nikata'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Nikata') OR e.normalized=lower('Nikata'))
);

-- PERSON | Nitha | refs: mn.116.piya.html:17, mn.116.piya.html:19
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Nitha', lower('Nitha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Nitha') OR e.normalized=lower('Nitha'))
);

-- PERSON | Opamanna | refs: dn.20.0.piya.html:30, dn.32.0.piya.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Opamanna', lower('Opamanna'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Opamanna') OR e.normalized=lower('Opamanna'))
);

-- PERSON | Paharada | refs: dn.20.0.piya.html:38, dn.20.0.than.html:48
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Paharada', lower('Paharada'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Paharada') OR e.normalized=lower('Paharada'))
);

-- PERSON | Panada | refs: dn.20.0.piya.html:30, dn.20.0.than.html:25
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Panada', lower('Panada'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Panada') OR e.normalized=lower('Panada'))
);

-- PERSON | Pandava | refs: snp.3.01.than.html:7, thag.01.00x.than.html:41
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Pandava', lower('Pandava'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Pandava') OR e.normalized=lower('Pandava'))
);

-- PERSON | Parileyyaka | refs: ud.4.05.irel.html:3, ud.4.05.irel.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Parileyyaka', lower('Parileyyaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Parileyyaka') OR e.normalized=lower('Parileyyaka'))
);

-- PERSON | Paritta | refs: dn.32.0.piya.html:4, dn.32.0.piya.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Paritta', lower('Paritta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Paritta') OR e.normalized=lower('Paritta'))
);

-- PERSON | Pataliputta | refs: dn.16.1-6.vaji.html:28, ud.8.06.than.html:29
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Pataliputta', lower('Pataliputta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Pataliputta') OR e.normalized=lower('Pataliputta'))
);

-- PERSON | Pilotika | refs: mn.027.than.html:1, mn.027.than.html:21
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Pilotika', lower('Pilotika'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Pilotika') OR e.normalized=lower('Pilotika'))
);

-- PERSON | Posala | refs: snp.5.14.than.html:1, snp.5.14.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Posala', lower('Posala'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Posala') OR e.normalized=lower('Posala'))
);

-- PERSON | Raja | refs: dn.32.0.piya.html:24, dn.32.0.piya.html:40
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Raja', lower('Raja'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Raja') OR e.normalized=lower('Raja'))
);

-- PERSON | Ramagama | refs: dn.16.1-6.vaji.html:37, dn.16.1-6.vaji.html:42, dn.16.5-6.than.html:136, dn.16.5-6.than.html:149
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Ramagama', lower('Ramagama'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Ramagama') OR e.normalized=lower('Ramagama'))
);

-- PERSON | Saavatthii the Brahman | refs: sn46.055.wlsh.html:1, sn48.042.wlsh.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Saavatthii the Brahman', lower('Saavatthii the Brahman'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Saavatthii the Brahman') OR e.normalized=lower('Saavatthii the Brahman'))
);

-- PERSON | Sadamatta | refs: dn.20.0.piya.html:54, dn.20.0.than.html:74
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sadamatta', lower('Sadamatta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sadamatta') OR e.normalized=lower('Sadamatta'))
);

-- PERSON | Sahali | refs: dn.20.0.piya.html:43, dn.20.0.than.html:55
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sahali', lower('Sahali'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sahali') OR e.normalized=lower('Sahali'))
);

-- PERSON | Salha | refs: an03.066.nymo.html:1, an03.066.nymo.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Salha', lower('Salha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Salha') OR e.normalized=lower('Salha'))
);

-- PERSON | Sanankumara | refs: dn.20.0.piya.html:61, dn.20.0.than.html:89
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sanankumara', lower('Sanankumara'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sanankumara') OR e.normalized=lower('Sanankumara'))
);

-- PERSON | Sangaarava | refs: sn07.021.wlsh.html:2, sn07.021.wlsh.html:8, sn46.055.wlsh.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sangaarava', lower('Sangaarava'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sangaarava') OR e.normalized=lower('Sangaarava'))
);

-- PERSON | Santusita | refs: dn.11.0.than.html:108, dn.11.0.than.html:109
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Santusita', lower('Santusita'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Santusita') OR e.normalized=lower('Santusita'))
);

-- PERSON | Santuttha | refs: dn.16.1-6.vaji.html:12, dn.16.1-6.vaji.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Santuttha', lower('Santuttha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Santuttha') OR e.normalized=lower('Santuttha'))
);

-- PERSON | Sesavati | refs: vv.3.07.irel.html:1, vv.3.07.irel.html:10, vv.3.07.irel.html:13
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sesavati', lower('Sesavati'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sesavati') OR e.normalized=lower('Sesavati'))
);

-- PERSON | Sigala | refs: dn.31.0.nara.html:13, dn.31.0.nara.html:38, dn.31.0.nara.html:7, dn.31.0.nara.html:8
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sigala', lower('Sigala'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sigala') OR e.normalized=lower('Sigala'))
);

-- PERSON | Siha | refs: an05.034.than.html:10, an05.034.than.html:3, an05.034.than.html:8
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Siha', lower('Siha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Siha') OR e.normalized=lower('Siha'))
);

-- PERSON | Sikhi | refs: dn.32.0.piya.html:1, dn.32.0.piya.html:4, mn.116.piya.html:26
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sikhi', lower('Sikhi'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sikhi') OR e.normalized=lower('Sikhi'))
);

-- PERSON | Sirima | refs: vv.1.16.irel.html:1, vv.1.16.irel.html:10, vv.1.16.irel.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sirima', lower('Sirima'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sirima') OR e.normalized=lower('Sirima'))
);

-- PERSON | Sister Sisupacala | refs: sn05.008.than.html:1, sn05.008.than.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sister Sisupacala', lower('Sister Sisupacala'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sister Sisupacala') OR e.normalized=lower('Sister Sisupacala'))
);

-- PERSON | Sister Upacala | refs: sn05.007.than.html:1, sn05.007.than.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sister Upacala', lower('Sister Upacala'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sister Upacala') OR e.normalized=lower('Sister Upacala'))
);

-- PERSON | Sopaka | refs: snp.1.07.piya.html:22, snp.1.07.piya.html:24
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sopaka', lower('Sopaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sopaka') OR e.normalized=lower('Sopaka'))
);

-- PERSON | Sucitti | refs: dn.20.0.piya.html:38, dn.20.0.than.html:47
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sucitti', lower('Sucitti'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sucitti') OR e.normalized=lower('Sucitti'))
);

-- PERSON | Sudatta | refs: dn.16.1-6.vaji.html:6, dn.16.1-6.vaji.html:9, sn10.008.than.html:10
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sudatta', lower('Sudatta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sudatta') OR e.normalized=lower('Sudatta'))
);

-- PERSON | Sujata | refs: dn.16.1-6.vaji.html:10, dn.16.1-6.vaji.html:6, ud.3.07.than.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sujata', lower('Sujata'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sujata') OR e.normalized=lower('Sujata'))
);

-- PERSON | Sumangala | refs: mn.116.piya.html:24, thag.01.00x.than.html:43, thig.02.03.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sumangala', lower('Sumangala'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sumangala') OR e.normalized=lower('Sumangala'))
);

-- PERSON | Sundara | refs: mn.116.piya.html:26, mn.116.piya.html:27
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sundara', lower('Sundara'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sundara') OR e.normalized=lower('Sundara'))
);

-- PERSON | Sundarika Bharadvaja | refs: mn.007.nypo.html:15, mn.007.nypo.html:16, mn.007.nypo.html:17, mn.007.nypo.html:19
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sundarika Bharadvaja', lower('Sundarika Bharadvaja'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sundarika Bharadvaja') OR e.normalized=lower('Sundarika Bharadvaja'))
);

-- PERSON | Sunimmita | refs: dn.11.0.than.html:110, dn.11.0.than.html:111
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sunimmita', lower('Sunimmita'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sunimmita') OR e.normalized=lower('Sunimmita'))
);

-- PERSON | Supremely Enlightened One | refs: dhp.04.budd.html:1, dhp.04.budd.html:59
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Supremely Enlightened One', lower('Supremely Enlightened One'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Supremely Enlightened One') OR e.normalized=lower('Supremely Enlightened One'))
);

-- PERSON | Sutava | refs: mn.116.piya.html:17, mn.116.piya.html:19
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sutava', lower('Sutava'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sutava') OR e.normalized=lower('Sutava'))
);

-- PERSON | Sutavan | refs: an09.007.than.html:1, an09.007.than.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sutavan', lower('Sutavan'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sutavan') OR e.normalized=lower('Sutavan'))
);

-- PERSON | Sāmaṇḍakāni | refs: an10.065.niza.html:1, an10.066.niza.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sāmaṇḍakāni', lower('Sāmaṇḍakāni'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sāmaṇḍakāni') OR e.normalized=lower('Sāmaṇḍakāni'))
);

-- PERSON | Sāmāvatī | refs: ud.7.10.than.html:2, ud.7.10.than.html:3
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Sāmāvatī', lower('Sāmāvatī'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Sāmāvatī') OR e.normalized=lower('Sāmāvatī'))
);

-- PERSON | Taayana | refs: sn02.008.wlsh.html:8, sn02.008.wlsh.html:9
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Taayana', lower('Taayana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Taayana') OR e.normalized=lower('Taayana'))
);

-- PERSON | Tagarasikhin | refs: ud.5.03.than.html:10
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Tagarasikhin', lower('Tagarasikhin'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Tagarasikhin') OR e.normalized=lower('Tagarasikhin'))
);

-- PERSON | Talaputa | refs: sn42.002.than.html:2, sn42.002.than.html:4, sn42.002.than.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Talaputa', lower('Talaputa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Talaputa') OR e.normalized=lower('Talaputa'))
);

-- PERSON | Tapussa | refs: an09.041.than.html:4, an09.041.than.html:6, an09.041.than.html:7
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Tapussa', lower('Tapussa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Tapussa') OR e.normalized=lower('Tapussa'))
);

-- PERSON | Tatha | refs: mn.116.piya.html:17, mn.116.piya.html:19, mn.116.piya.html:21
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Tatha', lower('Tatha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Tatha') OR e.normalized=lower('Tatha'))
);

-- PERSON | Todeyya | refs: snp.5.09.than.html:1, snp.5.09.than.html:2, snp.5.09.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Todeyya', lower('Todeyya'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Todeyya') OR e.normalized=lower('Todeyya'))
);

-- PERSON | Todeyya's son | refs: mn.135.nymo.html:21, mn.135.than.html:1, mn.135.than.html:22
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Todeyya''s son', lower('Todeyya''s son'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Todeyya''s son') OR e.normalized=lower('Todeyya''s son'))
);

-- PERSON | Tuttha | refs: dn.16.1-6.vaji.html:12, dn.16.1-6.vaji.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Tuttha', lower('Tuttha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Tuttha') OR e.normalized=lower('Tuttha'))
);

-- PERSON | Ugga | refs: an07.007.than.html:2, an07.007.than.html:3, an07.007.than.html:5
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Ugga', lower('Ugga'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Ugga') OR e.normalized=lower('Ugga'))
);

-- PERSON | Universal Seer | refs: dn.01.0.bodh.html:42, dn.01.0.bodh.html:43, dn.01.0.bodh.html:44
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Universal Seer', lower('Universal Seer'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Universal Seer') OR e.normalized=lower('Universal Seer'))
);

-- PERSON | Upaka | refs: mn.026.than.html:4, mn.026.than.html:53, mn.026.than.html:54, mn.026.than.html:56
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Upaka', lower('Upaka'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Upaka') OR e.normalized=lower('Upaka'))
);

-- PERSON | Uparittha | refs: mn.116.piya.html:17, mn.116.piya.html:19
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Uparittha', lower('Uparittha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Uparittha') OR e.normalized=lower('Uparittha'))
);

-- PERSON | Upatissa | refs: mn.024.than.html:40, mn.116.piya.html:26
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Upatissa', lower('Upatissa'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Upatissa') OR e.normalized=lower('Upatissa'))
);

-- PERSON | Vanquisher | refs: dn.01.0.bodh.html:42, dn.01.0.bodh.html:43, dn.01.0.bodh.html:44
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Vanquisher', lower('Vanquisher'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Vanquisher') OR e.normalized=lower('Vanquisher'))
);

-- PERSON | Varunas | refs: dn.20.0.than.html:51, dn.20.0.than.html:62
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Varunas', lower('Varunas'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Varunas') OR e.normalized=lower('Varunas'))
);

-- PERSON | Vasavatti Mara | refs: dn.20.0.piya.html:38, dn.20.0.piya.html:65
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Vasavatti Mara', lower('Vasavatti Mara'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Vasavatti Mara') OR e.normalized=lower('Vasavatti Mara'))
);

-- PERSON | Vehapphala | refs: an04.123.than.html:5, an04.125.than.html:5, mn.049.than.html:25
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Vehapphala', lower('Vehapphala'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Vehapphala') OR e.normalized=lower('Vehapphala'))
);

-- PERSON | Velukandaki | refs: an06.037.than.html:4
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Velukandaki', lower('Velukandaki'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Velukandaki') OR e.normalized=lower('Velukandaki'))
);

-- PERSON | Veluvana | refs: sn12.017x.wlsh.html:1, sn22.049.wlsh.html:1
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Veluvana', lower('Veluvana'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Veluvana') OR e.normalized=lower('Veluvana'))
);

-- PERSON | Ven. Elder | refs: sn21.010.than.html:12, sn21.010.than.html:6, sn21.010.than.html:7
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Ven. Elder', lower('Ven. Elder'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Ven. Elder') OR e.normalized=lower('Ven. Elder'))
);

-- PERSON | Ven. Sir | refs: sn46.014.piya.html:5, sn46.016.piya.html:6
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Ven. Sir', lower('Ven. Sir'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Ven. Sir') OR e.normalized=lower('Ven. Sir'))
);

-- PERSON | Vepacitta | refs: sn02.009.piya.html:9, sn02.010.piya.html:10
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Vepacitta', lower('Vepacitta'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Vepacitta') OR e.normalized=lower('Vepacitta'))
);

-- PERSON | Verahaccaani | refs: sn35.133.wlsh.html:1, sn35.133.wlsh.html:10, sn35.133.wlsh.html:2
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Verahaccaani', lower('Verahaccaani'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Verahaccaani') OR e.normalized=lower('Verahaccaani'))
);

-- PERSON | Verbal | refs: dn.21.2x.than.html:21, dn.21.2x.than.html:23
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Verbal', lower('Verbal'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Verbal') OR e.normalized=lower('Verbal'))
);

-- PERSON | Veroca | refs: dn.20.0.piya.html:39, dn.20.0.than.html:51
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Veroca', lower('Veroca'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Veroca') OR e.normalized=lower('Veroca'))
);

-- PERSON | Videhi queen | refs: dn.16.1-6.vaji.html:1, dn.16.1-6.vaji.html:33, dn.16.1-6.vaji.html:42
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Videhi queen', lower('Videhi queen'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Videhi queen') OR e.normalized=lower('Videhi queen'))
);

-- PERSON | bhikkhu Salha | refs: dn.16.1-6.vaji.html:6, dn.16.1-6.vaji.html:7
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'bhikkhu Salha', lower('bhikkhu Salha'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('bhikkhu Salha') OR e.normalized=lower('bhikkhu Salha'))
);

-- PERSON | honorable Koṇḍañña | refs: sn56.011.harv.html:20
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'honorable Koṇḍañña', lower('honorable Koṇḍañña'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('honorable Koṇḍañña') OR e.normalized=lower('honorable Koṇḍañña'))
);

-- PERSON | Ābhassara | refs: dn.01.0.bodh.html:40, dn.01.0.bodh.html:41
INSERT INTO ati_entities (entity_type, canonical, normalized, lang, notes, source, created_at, updated_at)
SELECT 'PERSON', 'Ābhassara', lower('Ābhassara'), 'en', 'Auto-generated from entities_to_fix split (likely_missing_canonical_entity).', 'manual', now(), now()
WHERE NOT EXISTS (
  SELECT 1 FROM ati_entities e
  WHERE e.entity_type = 'PERSON'
    AND (lower(e.canonical)=lower('Ābhassara') OR e.normalized=lower('Ābhassara'))
);


-- 2) Alias inserts for existing canonical
-- LOC | alias=bamboo grove monastery -> canonical=Bamboo-Grove Monastery | refs: sn36.021.than.html:4
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'bamboo grove monastery', lower('bamboo grove monastery'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'LOC'
  AND (lower(e.canonical)=lower('Bamboo-Grove Monastery') OR e.normalized=lower('Bamboo-Grove Monastery'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('bamboo grove monastery') OR a.normalized=lower('bamboo grove monastery'))
  );

-- LOC | alias=ghosita 's park -> canonical=Ghosita's Park | refs: an04.159.than.html:1, sn51.015.than.html:1
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'ghosita ''s park', lower('ghosita ''s park'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'LOC'
  AND (lower(e.canonical)=lower('Ghosita''s Park') OR e.normalized=lower('Ghosita''s Park'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('ghosita ''s park') OR a.normalized=lower('ghosita ''s park'))
  );

-- LOC | alias=maha-vana -> canonical=Great Forest | refs: dn.20.0.piya.html:2
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'maha-vana', lower('maha-vana'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'LOC'
  AND (lower(e.canonical)=lower('Great Forest') OR e.normalized=lower('Great Forest'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('maha-vana') OR a.normalized=lower('maha-vana'))
  );

-- LOC | alias=jeta 's grove -> canonical=Jeta's Grove | refs: sn35.101.than.html:8
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'jeta ''s grove', lower('jeta ''s grove'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'LOC'
  AND (lower(e.canonical)=lower('Jeta''s Grove') OR e.normalized=lower('Jeta''s Grove'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('jeta ''s grove') OR a.normalized=lower('jeta ''s grove'))
  );

-- LOC | alias=pavarika's mango grove -> canonical=Pavarika 's mango grove | refs: sn42.009.than.html:1
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'pavarika''s mango grove', lower('pavarika''s mango grove'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'LOC'
  AND (lower(e.canonical)=lower('Pavarika ''s mango grove') OR e.normalized=lower('Pavarika ''s mango grove'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('pavarika''s mango grove') OR a.normalized=lower('pavarika''s mango grove'))
  );

-- LOC | alias=vultures peak -> canonical=Vulture Peak Mountain | refs: thag.10.02.olen.html:1
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'vultures peak', lower('vultures peak'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'LOC'
  AND (lower(e.canonical)=lower('Vulture Peak Mountain') OR e.normalized=lower('Vulture Peak Mountain'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('vultures peak') OR a.normalized=lower('vultures peak'))
  );

-- LOC | alias=palace of migara 's mother -> canonical=palace of Migara's mother | refs: mn.118.than.html:1, mn.121.than.html:1
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'palace of migara ''s mother', lower('palace of migara ''s mother'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'LOC'
  AND (lower(e.canonical)=lower('palace of Migara''s mother') OR e.normalized=lower('palace of Migara''s mother'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('palace of migara ''s mother') OR a.normalized=lower('palace of migara ''s mother'))
  );

-- PERSON | alias=one self-awakened & -> canonical=Buddha | refs: snp.3.02.than.html:4
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'one self-awakened &', lower('one self-awakened &'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'PERSON'
  AND (lower(e.canonical)=lower('Buddha') OR e.normalized=lower('Buddha'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('one self-awakened &') OR a.normalized=lower('one self-awakened &'))
  );

-- PERSON | alias=mahabrahma -> canonical=Maha Brahma | refs: dn.20.0.piya.html:62
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'mahabrahma', lower('mahabrahma'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'PERSON'
  AND (lower(e.canonical)=lower('Maha Brahma') OR e.normalized=lower('Maha Brahma'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('mahabrahma') OR a.normalized=lower('mahabrahma'))
  );

-- PERSON | alias=ven.-moliya-phagguna -> canonical=Moliya-Phagguna | refs: sn12.012.than.html:2
INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
SELECT e.id, 'ven.-moliya-phagguna', lower('ven.-moliya-phagguna'), 'manual'
FROM ati_entities e
WHERE e.entity_type = 'PERSON'
  AND (lower(e.canonical)=lower('Moliya-Phagguna') OR e.normalized=lower('Moliya-Phagguna'))
  AND NOT EXISTS (
    SELECT 1 FROM ati_entity_aliases a
    WHERE a.entity_id = e.id
      AND (lower(a.alias)=lower('ven.-moliya-phagguna') OR a.normalized=lower('ven.-moliya-phagguna'))
  );

COMMIT;
