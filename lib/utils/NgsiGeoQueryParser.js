/**
 * NgsiQueryParser
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 * Class to parse geospatial data in NGSI-LD format
 * Focused only on parsing strings for georel cases: 'near' and 'within'
 */
var debug = require('debug')('iotbi.utils.NgsiGeoQueryParser');

class NgsiGeoQueryParser {
    /**
     * @param {Object} data - NGSI-LD data containing geospatial information as strings
     * @param {string} data.georel - Geographic relation ('near' or 'within')
     * @param {string} data.geometry - Geometry type ('Point', 'Polygon', etc.)
     * @param {string} data.coordinates - Coordinates as JSON string
     */
    constructor(georel,geometry,coordinates) {
        this.validateInput(georel,geometry,coordinates);
        
        // Parse string fields
        this.georel = georel.trim();
        this.geometry = geometry.trim();
        this.coordinates = this.parseCoordinates(coordinates);
        
        // Extract distances based on georel
        this.maxDistance = 0;
        this.minDistance = 0;
        
        this.parseGeoRel();
    }
    
    /**
     * Validate input data
     * @param {Object} data - Input data
     * @throws {Error} If data is not valid
     */
    validateInput(georel,geometry,coordinates) {
        if (!georel || typeof georel !== 'string') {
            throw new Error('Input georel must be an string');
        }
        if (!geometry || typeof geometry !== 'string') {
            throw new Error('Input geometry must be an string');
        }
        if (!coordinates || typeof coordinates !== 'string') {
            throw new Error('Input coordinates must be an string');
        }
        
        // Validate georel contains 'near' or 'within'
        const georelLower = georel.toLowerCase().trim();
        if (!georelLower.startsWith('near') && !georelLower.startsWith('within')) {
            throw new Error(`georel must contain 'near' or 'within'. Received: ${data.georel}`);
        }
    }
    
    /**
     * Parse coordinates string
     * @param {string} coordinatesString - String containing coordinates
     * @returns {Array} Parsed coordinates array
     */
    parseCoordinates(coordinatesString) {
        const trimmedString = coordinatesString.trim();
        
        try {
            // Try to parse as JSON
            const parsed = JSON.parse(trimmedString);
            
            if (!Array.isArray(parsed)) {
                throw new Error('Coordinates must be an array');
            }
            
            return parsed;
        } catch (jsonError) {
            // If not valid JSON, try manual parsing
            return this.parseCoordinatesManually(trimmedString);
        }
    }
    
    /**
     * Manual coordinates parsing (fallback for non-JSON formats)
     * @param {string} str - Coordinates string
     * @returns {Array} Coordinates array
     */
    parseCoordinatesManually(str) {
        // Remove outer brackets if they exist
        let cleanStr = str.trim();
        if (cleanStr.startsWith('[') && cleanStr.endsWith(']')) {
            cleanStr = cleanStr.slice(1, -1).trim();
        }
        
        // If empty string, return empty array
        if (!cleanStr) {
            return [];
        }
        
        try {
            // Try to parse as simple array [lon, lat]
            if (cleanStr.includes(',') && !cleanStr.includes('[')) {
                const parts = cleanStr.split(',').map(part => {
                    const num = parseFloat(part.trim());
                    return isNaN(num) ? 0 : num;
                });
                return parts;
            }
            
            // Try to parse as nested array
            const result = this.parseNestedArray(cleanStr);
            return result;
        } catch (error) {
            console.warn(`Error in manual coordinates parsing: ${error.message}`);
            return [];
        }
    }
    
    /**
     * Manual nested array parsing
     * @param {string} str - Array string
     * @returns {Array} Parsed array
     */
    parseNestedArray(str) {
        const result = [];
        let current = '';
        let depth = 0;
        let inNumber = false;
        
        for (let i = 0; i < str.length; i++) {
            const char = str[i];
            
            if (char === '[') {
                depth++;
                if (depth > 1) {
                    current += char;
                }
            } else if (char === ']') {
                depth--;
                if (depth === 0) {
                    if (current.trim()) {
                        const nested = this.parseNestedArray(current);
                        result.push(nested);
                        current = '';
                    }
                } else if (depth >= 1) {
                    current += char;
                }
            } else if (char === ',' && depth === 1) {
                if (current.trim()) {
                    const num = parseFloat(current.trim());
                    result.push(isNaN(num) ? 0 : num);
                    current = '';
                }
            } else {
                current += char;
            }
        }
        
        // Process last element
        if (current.trim() && depth === 0) {
            const num = parseFloat(current.trim());
            if (!isNaN(num)) {
                result.push(num);
            }
        }
        
        return result;
    }
    
    /**
     * Parse georel to extract distances
     */
    parseGeoRel() {
        const georelLower = this.georel.toLowerCase();
        
        if (georelLower.startsWith('near')) {
            this.parseNearGeoRel();
        } else if (georelLower.startsWith('within')) {
            // For 'within', there are no distances in the georel itself
            this.maxDistance = 0;
            this.minDistance = 0;
        }
    }
    
    /**
     * Parse 'near' georel to extract distances
     */
    parseNearGeoRel() {
        // Expected format: "near", "near;maxDistance==1000", or "near;maxDistance==1000;minDistance==100"
        const parts = this.georel.split(';').map(part => part.trim());
        
        // First part should be 'near'
        const relation = parts[0].toLowerCase();
        if (relation !== 'near') {
            return;
        }
        
        // Process parameters
        for (let i = 1; i < parts.length; i++) {
            const param = parts[i];
            const [key, value] = param.split('==').map(p => p.trim());
            
            if (key && value) {
                const paramKey = key.toLowerCase();
                const paramValue = parseFloat(value);
                
                if (!isNaN(paramValue)) {
                    if (paramKey === 'maxdistance') {
                        this.maxDistance = paramValue;
                    } else if (paramKey === 'mindistance') {
                        this.minDistance = paramValue;
                    }
                }
            }
        }
    }
    
    /**
     * Return parsed data
     * @returns {Object} Object with parsed data
     */
    getParsedData() {
        return {
            georel: this.georel,
            geometry: this.geometry,
            coordinates: this.coordinates,
            maxDistance: this.maxDistance,
            minDistance: this.minDistance
        };
    }
    
    /**
     * Return data as JSON string
     * @returns {string} Stringified JSON of parsed data
     */
    toJSON() {
        return JSON.stringify(this.getParsedData(), null, 2);
    }
    
    /**
     * Static method for quick parsing
     * @param {Object} data - NGSI-LD data
     * @returns {Object} Parsed data
     */
    static parse(data) {
        const parser = new NgsiGeoQueryParser(data);
        return parser.getParsedData();
    }
}

module.exports = NgsiGeoQueryParser;
