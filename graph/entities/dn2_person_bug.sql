-- missing DN entities
SELECT
  v.id,
  ve.mention,
  e->>'label' AS label,
  e->>'text'  AS text
FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS e
LEFT JOIN ati_verse_person ve
  ON ve.verse_id = v.id
WHERE v.nikaya = 'DN'
  AND v.book_number = 2
  AND e->>'label' = 'PERSON';

-- actual DN entities
  SELECT
  v.id,
  v.verse_num,
  e->>'text'  AS span_text,
  e->>'label' AS span_label,
  (e->>'start')::int AS span_start,
  (e->>'end')::int   AS span_end,
  ve.mention         AS ve_mention,
  ve.normalized      AS ve_normalized,
  ve.start_pos       AS ve_start,
  ve.end_pos         AS ve_end
FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS e
LEFT JOIN ati_verse_person ve
  ON  ve.verse_id = v.id
  AND ve.start_pos = (e->>'start')::int
  AND ve.end_pos   = (e->>'end')::int
WHERE v.nikaya = 'DN'
  AND v.book_number = 2
  AND e->>'label' = 'PERSON'
ORDER BY v.id, v.verse_num, span_start;



  id   | verse_num |       span_text       | span_label | span_start | span_end |      ve_mention       |     ve_normalized     | ve_start | ve_end 
-------+-----------+-----------------------+------------+------------+----------+-----------------------+-----------------------+----------+--------
 19767 |         1 | Blessed One           | PERSON     |         38 |       49 | Blessed One           | blessed one           |       38 |     49
 19767 |         1 | King Ajatasattu       | PERSON     |        295 |      310 | King Ajatasattu       | king ajatasattu       |      295 |    310
 19767 |         1 | Queen Videha          | PERSON     |        334 |      346 |                       |                       |          |       
 19768 |         2 | Purana Kassapa        | PERSON     |         83 |       97 |                       |                       |          |       
 19770 |         4 | Makkhali Gosala       | PERSON     |         64 |       79 |                       |                       |          |       
 19770 |         4 | Ajita Kesakambalin    | PERSON     |        107 |      125 |                       |                       |          |       
 19770 |         4 | Pakudha Kaccayana     | PERSON     |        153 |      170 |                       |                       |          |       
 19770 |         4 | Sañjaya Belatthaputta | PERSON     |        198 |      219 | Sañjaya Belatthaputta | sanjaya belatthaputta |      198 |    219
 19770 |         4 | Nigantha Nataputta    | PERSON     |        247 |      265 |                       |                       |          |       
 19772 |         6 | Jivaka                | PERSON     |         14 |       20 |                       |                       |          |       
 19772 |         6 | king. So              | PERSON     |         72 |       80 |                       |                       |          |       
 19772 |         6 | Jivaka                | PERSON     |        111 |      117 |                       |                       |          |       
 19773 |         7 | Blessed One           | PERSON     |         27 |       38 | Blessed One           | blessed one           |       27 |     38
 19773 |         7 | Blessed One           | PERSON     |        170 |      181 | Blessed One           | blessed one           |      170 |    181
 19773 |         7 | Blessed One           | PERSON     |        235 |      246 | Blessed One           | blessed one           |      235 |    246
 19774 |         8 | Jivaka                | PERSON     |         26 |       32 |                       |                       |          |       
 19775 |         9 | Jivaka                | PERSON     |        133 |      139 |                       |                       |          |       
 19776 |        10 | Jivaka Komarabhacca   | PERSON     |        248 |      267 |                       |                       |          |       
 19776 |        10 | Jivaka                | PERSON     |        456 |      462 |                       |                       |          |       
 19776 |        10 | Jivaka                | PERSON     |        485 |      491 |                       |                       |          |       
 19777 |        11 | king. Do              | PERSON     |         23 |       31 |                       |                       |          |       
 19778 |        12 | Jivaka                | PERSON     |        151 |      157 |                       |                       |          |       
 19778 |        12 | Jivaka                | PERSON     |        174 |      180 |                       |                       |          |       
 19778 |        12 | Blessed One           | PERSON     |        189 |      200 | Blessed One           | blessed one           |      189 |    200
 19779 |        13 | Blessed One           | PERSON     |         12 |       23 | Blessed One           | blessed one           |       12 |     23
 19780 |        14 | Blessed One           | PERSON     |         29 |       40 | Blessed One           | blessed one           |       29 |     40
 19780 |        14 | Prince Udayibhadda    | PERSON     |        233 |      251 |                       |                       |          |       
 19781 |        15 | Blessed One           | PERSON     |          5 |       16 | Blessed One           | blessed one           |        5 |     16
 19782 |        16 | Lord                  | PERSON     |          0 |        4 |                       |                       |          |       
 19782 |        16 | Prince Udayibhadda    | PERSON     |         14 |       32 |                       |                       |          |       
 19783 |        17 | Blessed One           | PERSON     |         25 |       36 | Blessed One           | blessed one           |       25 |     36
 19783 |        17 | Blessed One           | PERSON     |        178 |      189 | Blessed One           | blessed one           |      178 |    189
 19783 |        17 | Blessed One           | PERSON     |        216 |      227 | Blessed One           | blessed one           |      216 |    227
 19785 |        19 | Lord                  | PERSON     |          0 |        4 |                       |                       |          |       
 19789 |        23 | Blessed One           | PERSON     |         45 |       56 | Blessed One           | blessed one           |       45 |     56
 19789 |        23 | Blessed One           | PERSON     |         79 |       90 | Blessed One           | blessed one           |       79 |     90
 19791 |        25 | Purana Kassapa        | PERSON     |         25 |       39 |                       |                       |          |       
 19791 |        25 | Venerable Kassapa     | PERSON     |        209 |      226 |                       |                       |          |       
 19792 |        26 | Purana Kassapa        | PERSON     |         20 |       34 |                       |                       |          |       
 19793 |        27 | Purana Kassapa        | PERSON     |         80 |       94 |                       |                       |          |       
 19793 |        27 | Purana Kassapa        | PERSON     |        357 |      371 |                       |                       |          |       
 19793 |        27 | Purana Kassapa        | PERSON     |        547 |      561 |                       |                       |          |       
 19794 |        28 | Makkhali Gosala       | PERSON     |         26 |       41 |                       |                       |          |       
 19794 |        28 | Venerable Gosala      | PERSON     |        211 |      227 |                       |                       |          |       
 19795 |        29 | Makkhali Gosala       | PERSON     |         20 |       35 |                       |                       |          |       
 19798 |        32 | Makkhali Gosala       | PERSON     |         80 |       95 |                       |                       |          |       
 19798 |        32 | Makkhali Gosala       | PERSON     |        381 |      396 |                       |                       |          |       
 19798 |        32 | Makkhali Gosala       | PERSON     |        595 |      610 |                       |                       |          |       
 19799 |        33 | Ajita Kesakambalin    | PERSON     |         26 |       44 |                       |                       |          |       
 19799 |        33 | Venerable Ajita       | PERSON     |        214 |      229 |                       |                       |          |       
 19800 |        34 | Ajita Kesakambalin    | PERSON     |         20 |       38 |                       |                       |          |       
 19801 |        35 | Ajita Kesakambalin    | PERSON     |         80 |       98 |                       |                       |          |       
 19801 |        35 | Ajita Kesakambalin    | PERSON     |        363 |      381 |                       |                       |          |       
 19801 |        35 | Ajita Kesakambalin    | PERSON     |        559 |      577 |                       |                       |          |       
 19802 |        36 | Pakudha Kaccayana     | PERSON     |         26 |       43 |                       |                       |          |       
 19802 |        36 | Venerable Kaccayana   | PERSON     |        213 |      232 |                       |                       |          |       
 19803 |        37 | Pakudha Kaccayana     | PERSON     |         20 |       37 |                       |                       |          |       
 19805 |        39 | Pakudha Kaccayana     | PERSON     |         80 |       97 |                       |                       |          |       
 19805 |        39 | Pakudha Kaccayana     | PERSON     |        365 |      382 |                       |                       |          |       
 19805 |        39 | Pakudha Kaccayana     | PERSON     |        563 |      580 |                       |                       |          |       
 19806 |        40 | Nigantha Nataputta    | PERSON     |         26 |       44 |                       |                       |          |       
 19806 |        40 | Venerable Aggivessana | PERSON     |        214 |      235 |                       |                       |          |       
 19807 |        41 | Nigantha Nataputta    | PERSON     |         20 |       38 |                       |                       |          |       
 19807 |        41 | Nigantha              | PERSON     |         92 |      100 |                       |                       |          |       
 19807 |        41 | Nigantha              | PERSON     |        180 |      188 |                       |                       |          |       
 19807 |        41 | Nigantha              | PERSON     |        257 |      265 |                       |                       |          |       
 19807 |        41 | Nigantha              | PERSON     |        390 |      398 |                       |                       |          |       
 19807 |        41 | Nigantha              | PERSON     |        451 |      459 |                       |                       |          |       
 19807 |        41 | Knotless One          | PERSON     |        544 |      556 |                       |                       |          |       
 19807 |        41 | Nigantha              | PERSON     |        558 |      566 |                       |                       |          |       
 19807 |        41 | Nata                  | PERSON     |        578 |      582 |                       |                       |          |       
 19807 |        41 | Nataputta             | PERSON     |        584 |      593 |                       |                       |          |       
 19808 |        42 | Nigantha Nataputta    | PERSON     |         80 |       98 |                       |                       |          |       
 19808 |        42 | Nigantha Nataputta    | PERSON     |        369 |      387 |                       |                       |          |       
 19808 |        42 | Nigantha Nataputta    | PERSON     |        571 |      589 |                       |                       |          |       
 19809 |        43 | Sañjaya Belatthaputta | PERSON     |         26 |       47 | Sañjaya Belatthaputta | sanjaya belatthaputta |       26 |     47
 19809 |        43 | Venerable Sañjaya     | PERSON     |        217 |      234 |                       |                       |          |       
 19810 |        44 | Sañjaya Belatthaputta | PERSON     |         20 |       41 | Sañjaya Belatthaputta | sanjaya belatthaputta |       20 |     41
 19810 |        44 | Tathagata             | PERSON     |        509 |      518 | Tathagata             | tathagata             |      509 |    518
 19811 |        45 | Sañjaya Belatthaputta | PERSON     |         80 |      101 | Sañjaya Belatthaputta | sanjaya belatthaputta |       80 |    101
 19811 |        45 | Sañjaya Belatthaputta | PERSON     |        361 |      382 | Sañjaya Belatthaputta | sanjaya belatthaputta |      361 |    382
 19811 |        45 | Sañjaya Belatthaputta | PERSON     |        788 |      809 | Sañjaya Belatthaputta | sanjaya belatthaputta |      788 |    809
 19812 |        46 | Blessed One           | PERSON     |         20 |       31 | Blessed One           | blessed one           |       20 |     31
 19813 |        47 | king. But             | PERSON     |         18 |       27 |                       |                       |          |       
 19813 |        47 | King Ajatasattu       | PERSON     |        516 |      531 | King Ajatasattu       | king ajatasattu       |      516 |    531
 19813 |        47 | King Ajatasattu       | PERSON     |        584 |      599 | King Ajatasattu       | king ajatasattu       |      584 |    599
 19816 |        50 | king. With            | PERSON     |         28 |       38 |                       |                       |          |       
 19820 |        54 | king. But             | PERSON     |         18 |       27 |                       |                       |          |       
 19820 |        54 | King Ajatasattu       | PERSON     |        353 |      368 | King Ajatasattu       | king ajatasattu       |      353 |    368
 19820 |        54 | King Ajatasattu       | PERSON     |        421 |      436 | King Ajatasattu       | king ajatasattu       |      421 |    436
 19823 |        57 | king. With            | PERSON     |         28 |       38 |                       |                       |          |       
 19827 |        61 | king. Listen          | PERSON     |         18 |       30 |                       |                       |          |       
 19828 |        62 | Tathagata             | PERSON     |         39 |       48 | Tathagata             | tathagata             |       39 |     48
 19829 |        63 | Tathagata             | PERSON     |         80 |       89 | Tathagata             | tathagata             |       80 |     89