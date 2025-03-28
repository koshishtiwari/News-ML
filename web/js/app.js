/**
 * News Article Browser - Frontend Application
 * Handles UI interactions, API requests, and rendering for the news article browser
 */

// Configuration
const API_BASE_URL = '/api';
const ITEMS_PER_PAGE = 10;

// State management
const state = {
    articles: [],
    currentView: 'default',
    currentPage: 1,
    totalPages: 1,
    searchQuery: '',
    searchType: 'standard', // 'standard' or 'semantic'
    selectedCategory: null,
    selectedLocation: null
};

// DOM elements
const elements = {
    articlesContainer: document.getElementById('articlesContainer'),
    pagination: document.getElementById('pagination'),
    articlesCount: document.getElementById('articlesCount'),
    searchInput: document.getElementById('searchInput'),
    searchButton: document.getElementById('searchButton'),
    searchTypeText: document.getElementById('searchTypeText'),
    categoriesList: document.getElementById('categoriesList'),
    locationsList: document.getElementById('locationsList'),
    viewRecentBtn: document.getElementById('viewRecent'),
    loadingSpinner: document.getElementById('loadingSpinner'),
    loadingText: document.getElementById('loadingText'),
    
    // Modal elements
    articleModal: document.getElementById('articleModal'),
    modalTitle: document.getElementById('articleModalLabel'),
    modalContent: document.getElementById('modalContent'),
    modalSource: document.getElementById('modalSource'),
    modalLocation: document.getElementById('modalLocation'),
    modalCategory: document.getElementById('modalCategory'),
    modalDate: document.getElementById('modalDate'),
    modalSourceLink: document.getElementById('modalSourceLink'),
    similarArticles: document.getElementById('similarArticles'),
    
    // Stats modal
    statsTotalArticles: document.getElementById('statsTotalArticles'),
    statsLocationCount: document.getElementById('statsLocationCount'),
    statsSourceCount: document.getElementById('statsSourceCount'),
    statsCategoryCount: document.getElementById('statsCategoryCount'),
    statsOldestDate: document.getElementById('statsOldestDate'),
    statsNewestDate: document.getElementById('statsNewestDate')
};

/**
 * Show loading spinner with custom message
 * @param {string} message - The loading message to display
 */
function showLoading(message = 'Loading...') {
    elements.loadingText.textContent = message;
    elements.loadingSpinner.style.visibility = 'visible';
}

/**
 * Hide loading spinner
 */
function hideLoading() {
    elements.loadingSpinner.style.visibility = 'hidden';
}

/**
 * Format a date string for display
 * @param {string} dateString - ISO date string
 * @returns {string} Formatted date string
 */
function formatDate(dateString) {
    if (!dateString) return 'Unknown date';
    const date = new Date(dateString);
    return new Intl.DateTimeFormat('en-US', { 
        year: 'numeric', 
        month: 'short', 
        day: 'numeric',
        hour: 'numeric',
        minute: 'numeric'
    }).format(date);
}

/**
 * Create a truncated text preview
 * @param {string} text - The text to truncate
 * @param {number} maxLength - Maximum length
 * @returns {string} Truncated text
 */
function createTextPreview(text, maxLength = 300) {
    if (!text) return '';
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}

/**
 * Create an article card element
 * @param {Object} article - Article data
 * @returns {HTMLElement} Article card element
 */
function createArticleCard(article) {
    const card = document.createElement('div');
    card.className = 'card mb-3';
    
    // Add similarity score badge if available
    let similarityBadge = '';
    if (article.similarity_score) {
        const percent = Math.round(article.similarity_score * 100);
        similarityBadge = `
            <span class="badge bg-info float-end" title="Similarity score">
                ${percent}% match
            </span>
        `;
    }
    
    // Format badges
    const categoryBadge = article.category ? 
        `<span class="badge category-badge me-1">${article.category}</span>` : '';
    
    const locationBadge = article.location_query ? 
        `<span class="badge location-badge me-1">${article.location_query}</span>` : '';
    
    // Format date
    const dateStr = article.published_at ? formatDate(article.published_at) : 
                    (article.fetched_at ? formatDate(article.fetched_at) : 'Unknown date');
    
    // Create content preview
    const contentPreview = createTextPreview(article.content);
    
    // Build card HTML
    card.innerHTML = `
        <div class="card-header d-flex justify-content-between align-items-center">
            <h5 class="card-title mb-0">${article.title}</h5>
            ${similarityBadge}
        </div>
        <div class="card-body">
            <h6 class="card-subtitle mb-2 text-muted">
                ${article.source_name || 'Unknown source'}
            </h6>
            <p class="card-text">${contentPreview}</p>
        </div>
        <div class="card-footer d-flex justify-content-between align-items-center">
            <div>
                ${categoryBadge}
                ${locationBadge}
            </div>
            <small class="text-muted">${dateStr}</small>
        </div>
    `;
    
    // Add click handler to show article modal
    card.addEventListener('click', () => showArticleModal(article));
    
    return card;
}

/**
 * Render articles to the container
 * @param {Array} articles - Array of article data
 */
function renderArticles(articles) {
    // Clear the container
    elements.articlesContainer.innerHTML = '';
    elements.articlesCount.textContent = articles.length;
    
    if (articles.length === 0) {
        elements.articlesContainer.innerHTML = `
            <div class="alert alert-info">
                <i class="fas fa-info-circle me-2"></i>
                No articles found matching your criteria.
            </div>
        `;
        return;
    }
    
    // Get articles for the current page
    const startIndex = (state.currentPage - 1) * ITEMS_PER_PAGE;
    const endIndex = startIndex + ITEMS_PER_PAGE;
    const currentPageArticles = articles.slice(startIndex, endIndex);
    
    // Render each article
    currentPageArticles.forEach(article => {
        const articleCard = createArticleCard(article);
        elements.articlesContainer.appendChild(articleCard);
    });
    
    // Update pagination
    renderPagination(articles.length);
}

/**
 * Render pagination controls
 * @param {number} totalItems - Total number of items
 */
function renderPagination(totalItems) {
    elements.pagination.innerHTML = '';
    
    // Calculate total pages
    state.totalPages = Math.ceil(totalItems / ITEMS_PER_PAGE);
    
    // Don't show pagination if only one page
    if (state.totalPages <= 1) {
        return;
    }
    
    // Create previous button
    const prevLi = document.createElement('li');
    prevLi.className = `page-item ${state.currentPage === 1 ? 'disabled' : ''}`;
    prevLi.innerHTML = `
        <a class="page-link" href="#" aria-label="Previous">
            <span aria-hidden="true">&laquo;</span>
        </a>
    `;
    prevLi.addEventListener('click', (e) => {
        e.preventDefault();
        if (state.currentPage > 1) {
            state.currentPage--;
            renderArticles(state.articles);
        }
    });
    elements.pagination.appendChild(prevLi);
    
    // Create page number buttons
    // Limit to max 5 pages for better UI
    let startPage = Math.max(1, state.currentPage - 2);
    let endPage = Math.min(state.totalPages, startPage + 4);
    
    // Adjust startPage if we're near the end
    if (endPage - startPage < 4) {
        startPage = Math.max(1, endPage - 4);
    }
    
    for (let i = startPage; i <= endPage; i++) {
        const pageLi = document.createElement('li');
        pageLi.className = `page-item ${i === state.currentPage ? 'active' : ''}`;
        pageLi.innerHTML = `<a class="page-link" href="#">${i}</a>`;
        pageLi.addEventListener('click', (e) => {
            e.preventDefault();
            state.currentPage = i;
            renderArticles(state.articles);
        });
        elements.pagination.appendChild(pageLi);
    }
    
    // Create next button
    const nextLi = document.createElement('li');
    nextLi.className = `page-item ${state.currentPage === state.totalPages ? 'disabled' : ''}`;
    nextLi.innerHTML = `
        <a class="page-link" href="#" aria-label="Next">
            <span aria-hidden="true">&raquo;</span>
        </a>
    `;
    nextLi.addEventListener('click', (e) => {
        e.preventDefault();
        if (state.currentPage < state.totalPages) {
            state.currentPage++;
            renderArticles(state.articles);
        }
    });
    elements.pagination.appendChild(nextLi);
}

/**
 * Show article modal with detailed content
 * @param {Object} article - Article data
 */
async function showArticleModal(article) {
    // Update modal content
    elements.modalTitle.textContent = article.title;
    elements.modalContent.innerHTML = article.content ? article.content.replace(/\n/g, '<br>') : 'No content available';
    elements.modalSource.textContent = article.source_name || 'Unknown source';
    elements.modalLocation.textContent = article.location_query || 'No location';
    elements.modalCategory.textContent = article.category || 'Uncategorized';
    elements.modalDate.textContent = article.published_at ? formatDate(article.published_at) : 'Unknown date';
    elements.modalSourceLink.href = article.url;
    
    // Show the modal
    const articleModal = new bootstrap.Modal(elements.articleModal);
    articleModal.show();
    
    // Load similar articles
    try {
        elements.similarArticles.innerHTML = '<div class="text-center"><div class="spinner-border spinner-border-sm" role="status"></div> Loading similar articles...</div>';
        
        const response = await fetch(`${API_BASE_URL}/articles/similar/${encodeURIComponent(article.url)}`);
        if (!response.ok) {
            throw new Error('Failed to fetch similar articles');
        }
        
        const similarArticles = await response.json();
        
        if (similarArticles.length === 0) {
            elements.similarArticles.innerHTML = '<div class="text-muted">No similar articles found</div>';
            return;
        }
        
        // Render similar articles
        elements.similarArticles.innerHTML = '';
        similarArticles.forEach(similarArticle => {
            if (similarArticle.url === article.url) return; // Skip current article
            
            const percent = Math.round(similarArticle.similarity_score * 100);
            const item = document.createElement('a');
            item.className = 'list-group-item list-group-item-action';
            item.href = '#';
            item.innerHTML = `
                <div class="d-flex w-100 justify-content-between">
                    <h6 class="mb-1">${similarArticle.title}</h6>
                    <span class="badge bg-info">${percent}%</span>
                </div>
                <small class="text-muted">${similarArticle.source_name} - ${formatDate(similarArticle.published_at)}</small>
            `;
            item.addEventListener('click', (e) => {
                e.preventDefault();
                articleModal.hide();
                setTimeout(() => showArticleModal(similarArticle), 500);
            });
            elements.similarArticles.appendChild(item);
        });
        
    } catch (error) {
        console.error('Error loading similar articles:', error);
        elements.similarArticles.innerHTML = '<div class="text-danger">Error loading similar articles</div>';
    }
}

/**
 * Fetch articles based on the current state
 */
async function fetchArticles() {
    showLoading('Loading articles...');
    
    try {
        let url;
        let params = [];
        
        // Determine the endpoint based on current state
        if (state.searchQuery && state.searchType === 'semantic') {
            url = `${API_BASE_URL}/search/semantic`;
            params.push(`query=${encodeURIComponent(state.searchQuery)}`);
            params.push(`limit=50`); // Higher limit for semantic search
        } else if (state.searchQuery) {
            url = `${API_BASE_URL}/articles/search`;
            params.push(`keyword=${encodeURIComponent(state.searchQuery)}`);
            params.push(`limit=50`); 
        } else if (state.selectedLocation) {
            url = `${API_BASE_URL}/articles/location/${encodeURIComponent(state.selectedLocation)}`;
            params.push(`limit=50`);
        } else {
            // Default to recent articles
            url = `${API_BASE_URL}/articles/recent`;
            params.push(`limit=50`);
        }
        
        // Add parameters as query string
        if (params.length > 0) {
            url += `?${params.join('&')}`;
        }
        
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error(`API returned status ${response.status}`);
        }
        
        state.articles = await response.json();
        state.currentPage = 1; // Reset to first page
        renderArticles(state.articles);
        
    } catch (error) {
        console.error('Error fetching articles:', error);
        elements.articlesContainer.innerHTML = `
            <div class="alert alert-danger">
                <i class="fas fa-exclamation-triangle me-2"></i>
                Error loading articles: ${error.message}
            </div>
        `;
        elements.articlesCount.textContent = 0;
    } finally {
        hideLoading();
    }
}

/**
 * Fetch and render locations
 */
async function fetchLocations() {
    try {
        const response = await fetch(`${API_BASE_URL}/locations`);
        
        if (!response.ok) {
            throw new Error(`API returned status ${response.status}`);
        }
        
        const locations = await response.json();
        
        // Render locations list
        elements.locationsList.innerHTML = '';
        
        if (locations.length === 0) {
            elements.locationsList.innerHTML = `
                <li class="list-group-item">No locations available</li>
            `;
            return;
        }
        
        locations.forEach(location => {
            const locationItem = document.createElement('li');
            locationItem.className = 'list-group-item location-item d-flex justify-content-between align-items-center';
            locationItem.innerHTML = `
                <span>${location}</span>
                <i class="fas fa-map-marker-alt text-success"></i>
            `;
            
            locationItem.addEventListener('click', () => {
                // Update state and fetch articles
                state.selectedLocation = location;
                state.selectedCategory = null;
                state.searchQuery = '';
                elements.searchInput.value = '';
                
                // Highlight the selected location
                document.querySelectorAll('.location-item').forEach(el => {
                    el.classList.remove('active');
                });
                locationItem.classList.add('active');
                
                fetchArticles();
            });
            
            elements.locationsList.appendChild(locationItem);
        });
        
    } catch (error) {
        console.error('Error fetching locations:', error);
        elements.locationsList.innerHTML = `
            <li class="list-group-item text-danger">Error loading locations</li>
        `;
    }
}

/**
 * Fetch and render categories
 */
async function fetchCategories() {
    try {
        const response = await fetch(`${API_BASE_URL}/categories`);
        
        if (!response.ok) {
            throw new Error(`API returned status ${response.status}`);
        }
        
        const categoriesData = await response.json();
        const categories = Object.entries(categoriesData);
        
        // Render categories list
        elements.categoriesList.innerHTML = '';
        
        if (categories.length === 0) {
            elements.categoriesList.innerHTML = `
                <li class="list-group-item">No categories available</li>
            `;
            return;
        }
        
        // Sort by count descending
        categories.sort((a, b) => b[1] - a[1]);
        
        categories.forEach(([category, count]) => {
            const categoryItem = document.createElement('li');
            categoryItem.className = 'list-group-item category-item d-flex justify-content-between align-items-center';
            categoryItem.innerHTML = `
                <span>${category || 'Uncategorized'}</span>
                <span class="badge rounded-pill bg-primary">${count}</span>
            `;
            
            categoryItem.addEventListener('click', () => {
                // TODO: Fetch articles by category (need API endpoint)
                alert('Category filtering not yet implemented');
            });
            
            elements.categoriesList.appendChild(categoryItem);
        });
        
    } catch (error) {
        console.error('Error fetching categories:', error);
        elements.categoriesList.innerHTML = `
            <li class="list-group-item text-danger">Error loading categories</li>
        `;
    }
}

/**
 * Fetch and display statistics
 */
async function fetchStats() {
    try {
        const response = await fetch(`${API_BASE_URL}/stats`);
        
        if (!response.ok) {
            throw new Error(`API returned status ${response.status}`);
        }
        
        const stats = await response.json();
        
        // Update stats in modal
        elements.statsTotalArticles.textContent = stats.total_articles || 0;
        elements.statsLocationCount.textContent = stats.location_count || 0;
        elements.statsSourceCount.textContent = stats.source_count || 0;
        elements.statsCategoryCount.textContent = stats.category_count || 0;
        
        if (stats.oldest_article) {
            elements.statsOldestDate.textContent = formatDate(stats.oldest_article);
        } else {
            elements.statsOldestDate.textContent = 'N/A';
        }
        
        if (stats.newest_article) {
            elements.statsNewestDate.textContent = formatDate(stats.newest_article);
        } else {
            elements.statsNewestDate.textContent = 'N/A';
        }
        
    } catch (error) {
        console.error('Error fetching stats:', error);
        alert('Error loading statistics');
    }
}

/**
 * Initialize the application
 */
function init() {
    // Load initial data
    fetchArticles();
    fetchLocations();
    fetchCategories();
    
    // Set up event listeners
    elements.searchInput.addEventListener('keyup', (e) => {
        if (e.key === 'Enter') {
            state.searchQuery = elements.searchInput.value.trim();
            state.selectedLocation = null;
            state.selectedCategory = null;
            
            // Remove active class from location and category items
            document.querySelectorAll('.location-item, .category-item').forEach(el => {
                el.classList.remove('active');
            });
            
            fetchArticles();
        }
    });
    
    elements.searchButton.addEventListener('click', () => {
        state.searchQuery = elements.searchInput.value.trim();
        state.selectedLocation = null;
        state.selectedCategory = null;
        
        // Remove active class from location and category items
        document.querySelectorAll('.location-item, .category-item').forEach(el => {
            el.classList.remove('active');
        });
        
        fetchArticles();
    });
    
    // Handle search type dropdown selection
    document.querySelectorAll('.search-type').forEach(el => {
        el.addEventListener('click', (e) => {
            e.preventDefault();
            const searchType = el.getAttribute('data-type');
            state.searchType = searchType;
            elements.searchTypeText.textContent = searchType === 'semantic' ? 'Semantic' : 'Standard';
        });
    });
    
    elements.viewRecentBtn.addEventListener('click', () => {
        state.searchQuery = '';
        state.selectedLocation = null;
        state.selectedCategory = null;
        elements.searchInput.value = '';
        
        // Remove active class from location and category items
        document.querySelectorAll('.location-item, .category-item').forEach(el => {
            el.classList.remove('active');
        });
        
        fetchArticles();
    });
    
    // Stats modal listeners
    elements.statsModal.addEventListener('show.bs.modal', () => {
        fetchStats();
    });
}

// Start the application when page is loaded
document.addEventListener('DOMContentLoaded', init);