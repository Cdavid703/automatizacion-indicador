/**
 * Automatización de indicador en Google Sheets
 *
 * Este script:
 * 1. Limpia el rango de destino en la hoja Data sin borrar encabezados.
 * 2. Elimina y recrea las hojas Audit_update y Audit.
 * 3. Trae información desde la hoja RAW del archivo origen.
 * 4. Enriquece los registros con Customer, CC y Classification.
 * 5. Filtra los registros con clasificación ECAN.
 * 6. Copia el resultado a Audit y luego a Data.
 * 7. Copia la columna E hacia AJ y la convierte a formato numérico.
 *
 * IMPORTANTE:
 * Reemplazar los IDs de archivo antes de ejecutar.
 */

const CONFIG = {
  // Reemplazar por el ID real del archivo destino
  DESTINATION_FILE_ID: 'REEMPLAZAR_CON_ID_ARCHIVO_DESTINO',

  // Reemplazar por el ID real del archivo origen
  SOURCE_FILE_ID: 'REEMPLAZAR_CON_ID_ARCHIVO_ORIGEN',

  MENU_NAME: 'Disputes',
  DATA_SHEET_NAME: 'Data',
  RAW_SHEET_NAME: 'RAW',
  AUDIT_UPDATE_SHEET_NAME: 'Audit_update',
  AUDIT_SHEET_NAME: 'Audit',

  HEADER_ROW: 1,
  FIRST_DATA_ROW: 2,

  // RAW!J:AB
  RAW_CORE_START_COL: 10,
  RAW_CORE_NUM_COLS: 19,

  // Data!I:AA
  DATA_TARGET_START_COL: 9,
  DATA_TARGET_NUM_COLS: 19,

  // Columnas auxiliares en Data
  DATA_COLUMN_E: 5,
  DATA_COLUMN_AJ: 36,

  FILTER_VALUE: 'ECAN',
  HEADER_BLUE: '#1F4E78',

  // Formato numérico final para AJ
  AJ_FINAL_NUMBER_FORMAT: '#,##0.###############',

  AUDIT_UPDATE_HEADERS: [
    'Problem No.',
    'Customer No.',
    'Company',
    'Tran Type',
    'Disputed',
    'Invoice Amount',
    'Original Invoice',
    'Ref No.',
    'Days GP',
    'Status',
    'Entered',
    'Identified',
    'Owner',
    'Problem Owner Name',
    'Reason',
    'Reason Description',
    'Sales Area',
    'Problem Sales Area Description',
    'PNote',
    'Customer',
    'CC',
    'Classification'
  ],

  AUDIT_UPDATE_COLUMN_WIDTHS: [
    120, 120, 120, 110, 110, 120, 120, 120, 90, 100, 100,
    100, 120, 160, 120, 220, 120, 220, 180, 110, 90, 120
  ],

  AUDIT_COLUMN_WIDTHS: [
    120, 120, 120, 110, 110, 120, 120, 120, 90, 100, 100,
    100, 120, 160, 120, 220, 120, 220, 180
  ]
};

const CLASSIFICATION_CODES = {
  ECAN: ['CA71', 'CA92', 'CA93', 'CA94', 'CA25'],
  WCAN: ['CA31', 'CA33', 'CA34', 'CA35', 'CA36', 'CA41', 'CA42', 'CA43', 'US31', 'US96', 'CA47', 'CA48', 'CA39', 'CA38', 'CA44'],
  USCEM: ['US11', 'US21', 'US27', 'US29', 'CA26'],
  USACM: ['US52', 'US55', 'US59', 'US61', 'US63', 'US64', 'US68', 'US71', 'US72', 'US73', 'US92', 'US93', 'US94', 'US95']
};

const CLASSIFICATION_LOOKUP = buildClassificationLookup_();

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu(CONFIG.MENU_NAME)
    .addItem('Actualizar indicador', 'actualizarIndicador')
    .addItem('Limpiar remanentes de Indicador', 'limpiarRemanentesIndicador')
    .addToUi();
}

function actualizarIndicador() {
  const ui = SpreadsheetApp.getUi();
  const lock = LockService.getDocumentLock();

  try {
    lock.waitLock(30000);

    const destinationSs = SpreadsheetApp.getActiveSpreadsheet();
    validateDestinationSpreadsheet_(destinationSs);

    const sourceSs = SpreadsheetApp.openById(CONFIG.SOURCE_FILE_ID);

    const dataSheet = getSheetOrThrow_(destinationSs, CONFIG.DATA_SHEET_NAME);
    const rawSheet = getSheetOrThrow_(sourceSs, CONFIG.RAW_SHEET_NAME);

    // 1. Limpiar Data!I:AA sin tocar encabezados
    clearRangeBelowHeader_(dataSheet, CONFIG.DATA_TARGET_START_COL, CONFIG.DATA_TARGET_NUM_COLS);

    // 2. Eliminar y recrear Audit_update y Audit
    recreateAuditSheets_(destinationSs);

    const auditUpdateSheet = getSheetOrThrow_(destinationSs, CONFIG.AUDIT_UPDATE_SHEET_NAME);
    const auditSheet = getSheetOrThrow_(destinationSs, CONFIG.AUDIT_SHEET_NAME);

    // 3. Crear cabeceras de Audit_update
    setupHeader_(auditUpdateSheet, CONFIG.AUDIT_UPDATE_HEADERS);

    // 4. Leer RAW!J:AB
    const coreRows = getRawCoreRows_(rawSheet);

    // 5. Construir filas enriquecidas con Customer, CC y Classification
    const auditUpdateRows = buildAuditUpdateRows_(coreRows);

    if (auditUpdateRows.length > 0) {
      auditUpdateSheet
        .getRange(CONFIG.FIRST_DATA_ROW, 1, auditUpdateRows.length, CONFIG.AUDIT_UPDATE_HEADERS.length)
        .setValues(auditUpdateRows);
    }

    // 6. Aplicar formato a Audit_update
    formatSheetColumns_(auditUpdateSheet, CONFIG.AUDIT_UPDATE_COLUMN_WIDTHS);

    // 7. Filtrar Classification = ECAN y copiar A:S a Audit
    const auditRows = buildAuditRowsFromAuditUpdateRows_(auditUpdateRows);
    writeAuditSheet_(auditSheet, auditRows);

    // 8. Copiar Audit!A:S hacia Data!I:AA
    if (auditRows.length > 0) {
      dataSheet
        .getRange(
          CONFIG.FIRST_DATA_ROW,
          CONFIG.DATA_TARGET_START_COL,
          auditRows.length,
          CONFIG.DATA_TARGET_NUM_COLS
        )
        .setValues(auditRows);
    }

    // 9. Copiar Data!E:E a Data!AJ:AJ y convertir a número
    refreshAjFromColumnE_(dataSheet);

    SpreadsheetApp.flush();
    ui.alert('Indicador actualizado');
  } catch (error) {
    ui.alert('Se detuvo el proceso.\n\n' + error.message);
    throw error;
  } finally {
    try {
      lock.releaseLock();
    } catch (e) {
      // sin acción
    }
  }
}

function limpiarRemanentesIndicador() {
  const ui = SpreadsheetApp.getUi();
  const lock = LockService.getDocumentLock();

  try {
    lock.waitLock(30000);

    const destinationSs = SpreadsheetApp.getActiveSpreadsheet();
    validateDestinationSpreadsheet_(destinationSs);

    deleteSheetIfExists_(destinationSs, CONFIG.AUDIT_UPDATE_SHEET_NAME);
    deleteSheetIfExists_(destinationSs, CONFIG.AUDIT_SHEET_NAME);

    SpreadsheetApp.flush();
    ui.alert('Limpieza de remanentes de Indicador finalizada');
  } catch (error) {
    ui.alert('Se detuvo la limpieza.\n\n' + error.message);
    throw error;
  } finally {
    try {
      lock.releaseLock();
    } catch (e) {
      // sin acción
    }
  }
}

function validateDestinationSpreadsheet_(spreadsheet) {
  if (spreadsheet.getId() !== CONFIG.DESTINATION_FILE_ID) {
    throw new Error('Este script debe ejecutarse dentro del archivo destino correcto.');
  }
}

function recreateAuditSheets_(spreadsheet) {
  deleteSheetIfExists_(spreadsheet, CONFIG.AUDIT_UPDATE_SHEET_NAME);
  deleteSheetIfExists_(spreadsheet, CONFIG.AUDIT_SHEET_NAME);

  spreadsheet.insertSheet(CONFIG.AUDIT_UPDATE_SHEET_NAME);
  spreadsheet.insertSheet(CONFIG.AUDIT_SHEET_NAME);
}

function deleteSheetIfExists_(spreadsheet, sheetName) {
  const sheet = spreadsheet.getSheetByName(sheetName);
  if (sheet) {
    spreadsheet.deleteSheet(sheet);
  }
}

function getSheetOrThrow_(spreadsheet, sheetName) {
  const sheet = spreadsheet.getSheetByName(sheetName);
  if (!sheet) {
    throw new Error('No encontré la pestaña "' + sheetName + '".');
  }
  return sheet;
}

function clearRangeBelowHeader_(sheet, startCol, numCols) {
  const maxRows = sheet.getMaxRows();
  if (maxRows >= CONFIG.FIRST_DATA_ROW) {
    sheet
      .getRange(CONFIG.FIRST_DATA_ROW, startCol, maxRows - 1, numCols)
      .clearContent();
  }
}

function setupHeader_(sheet, headers) {
  sheet.clear();

  sheet
    .getRange(1, 1, 1, headers.length)
    .setValues([headers])
    .setBackground(CONFIG.HEADER_BLUE)
    .setFontColor('#FFFFFF')
    .setFontWeight('bold')
    .setVerticalAlignment('middle')
    .setHorizontalAlignment('center');

  sheet.setFrozenRows(1);
  sheet.setRowHeight(1, 42);
}

function formatSheetColumns_(sheet, columnWidths) {
  for (let col = 0; col < columnWidths.length; col++) {
    sheet.setColumnWidth(col + 1, columnWidths[col]);
  }
}

function getRawCoreRows_(rawSheet) {
  const rawLastRow = rawSheet.getLastRow();
  if (rawLastRow < CONFIG.FIRST_DATA_ROW) {
    return [];
  }

  const values = rawSheet
    .getRange(
      CONFIG.FIRST_DATA_ROW,
      CONFIG.RAW_CORE_START_COL,
      rawLastRow - 1,
      CONFIG.RAW_CORE_NUM_COLS
    )
    .getValues();

  return values.filter(function (row) {
    return !isRowEmpty_(row);
  });
}

function buildAuditUpdateRows_(coreRows) {
  if (!coreRows || coreRows.length === 0) {
    return [];
  }

  const enrichedRows = coreRows.map(function (row) {
    const customerNo = normalize_(row[1]);
    const customer = extractCustomer_(customerNo);
    const cc = extractCc_(customerNo);
    const classification = classifyCc_(cc);

    return row.concat([customer, cc, classification]);
  });

  enrichedRows.sort(function (a, b) {
    const ccA = normalize_(a[20]).toUpperCase();
    const ccB = normalize_(b[20]).toUpperCase();
    return ccA.localeCompare(ccB);
  });

  return enrichedRows;
}

function extractCustomer_(value) {
  const text = normalize_(value);
  if (text === '') {
    return ' ';
  }

  const firstTen = text.substring(0, 10);
  const lastFive = firstTen.length >= 5 ? firstTen.substring(firstTen.length - 5) : firstTen;

  return lastFive || ' ';
}

function extractCc_(value) {
  const text = normalize_(value);
  if (text === '') {
    return ' ';
  }

  const lastFour = text.length >= 4 ? text.substring(text.length - 4) : text;
  return lastFour || ' ';
}

function classifyCc_(cc) {
  const key = normalize_(cc).toUpperCase();
  return CLASSIFICATION_LOOKUP[key] || ' ';
}

function buildClassificationLookup_() {
  const lookup = {};

  Object.keys(CLASSIFICATION_CODES).forEach(function (classification) {
    CLASSIFICATION_CODES[classification].forEach(function (code) {
      lookup[String(code).toUpperCase()] = classification;
    });
  });

  return lookup;
}

function buildAuditRowsFromAuditUpdateRows_(auditUpdateRows) {
  if (!auditUpdateRows || auditUpdateRows.length === 0) {
    return [];
  }

  return auditUpdateRows
    .filter(function (row) {
      return normalize_(row[21]).toUpperCase() === CONFIG.FILTER_VALUE;
    })
    .map(function (row) {
      return row.slice(0, 19);
    });
}

function writeAuditSheet_(auditSheet, auditRows) {
  const auditHeaders = CONFIG.AUDIT_UPDATE_HEADERS.slice(0, 19);

  setupHeader_(auditSheet, auditHeaders);

  if (auditRows.length > 0) {
    auditSheet
      .getRange(CONFIG.FIRST_DATA_ROW, 1, auditRows.length, 19)
      .setValues(auditRows);
  }

  formatSheetColumns_(auditSheet, CONFIG.AUDIT_COLUMN_WIDTHS);
}

function refreshAjFromColumnE_(dataSheet) {
  clearRangeBelowHeader_(dataSheet, CONFIG.DATA_COLUMN_AJ, 1);

  const lastRowInE = getLastDataRowInColumn_(dataSheet, CONFIG.DATA_COLUMN_E);
  if (lastRowInE < CONFIG.FIRST_DATA_ROW) {
    return;
  }

  const sourceValues = dataSheet
    .getRange(
      CONFIG.FIRST_DATA_ROW,
      CONFIG.DATA_COLUMN_E,
      lastRowInE - 1,
      1
    )
    .getValues();

  const normalizedValues = sourceValues.map(function (row) {
    return [coerceToNumberIfPossible_(row[0])];
  });

  const targetRange = dataSheet.getRange(
    CONFIG.FIRST_DATA_ROW,
    CONFIG.DATA_COLUMN_AJ,
    normalizedValues.length,
    1
  );

  targetRange.setValues(normalizedValues);
  targetRange.setNumberFormat(CONFIG.AJ_FINAL_NUMBER_FORMAT);

  dataSheet.setColumnWidth(CONFIG.DATA_COLUMN_AJ, 120);
}

function getLastDataRowInColumn_(sheet, columnIndex) {
  const lastRow = sheet.getLastRow();
  if (lastRow < CONFIG.FIRST_DATA_ROW) {
    return 1;
  }

  const values = sheet
    .getRange(CONFIG.FIRST_DATA_ROW, columnIndex, lastRow - 1, 1)
    .getValues();

  for (let i = values.length - 1; i >= 0; i--) {
    if (String(values[i][0]).trim() !== '') {
      return i + CONFIG.FIRST_DATA_ROW;
    }
  }

  return 1;
}

function coerceToNumberIfPossible_(value) {
  if (value === '' || value === null) {
    return '';
  }

  if (typeof value === 'number') {
    return value;
  }

  const raw = String(value).trim();
  if (raw === '') {
    return '';
  }

  const cleaned = raw.replace(/\s+/g, '').replace(/,/g, '');
  const numericValue = Number(cleaned);

  return Number.isNaN(numericValue) ? raw : numericValue;
}

function isRowEmpty_(row) {
  return row.every(function (value) {
    return normalize_(value) === '';
  });
}

function normalize_(value) {
  return String(value === null || value === undefined ? '' : value).trim();
}
