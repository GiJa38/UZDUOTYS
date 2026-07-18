/* ==========================================================================
   APPLICATION CONTROLLER - AI STUDIJŲ SRAUTAS
   ========================================================================== */

// Helper: Generate Unique ID
function generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
}

// ==========================================================================
// DATABASE MANAGER (IndexedDB Wrapper for Stream)
// ==========================================================================
class DatabaseManager {
    constructor() {
        this.dbName = 'AIStudyStreamDB';
        this.dbVersion = 1;
        this.db = null;
    }

    init() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, this.dbVersion);

            request.onerror = (event) => {
                console.error('Database error:', event.target.error);
                reject(event.target.error);
            };

            request.onsuccess = (event) => {
                this.db = event.target.result;
                resolve(this.db);
            };

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                
                // Create Topics Store
                if (!db.objectStoreNames.contains('topics')) {
                    db.createObjectStore('topics', { keyPath: 'id' });
                }
                
                // Create Stream Items Store (Holds notes, codes, links, images chronologically)
                if (!db.objectStoreNames.contains('stream_items')) {
                    const streamStore = db.createObjectStore('stream_items', { keyPath: 'id' });
                    streamStore.createIndex('topicId', 'topicId', { unique: false });
                }
            };
        });
    }

    _getTransaction(storeName, mode = 'readonly') {
        const transaction = this.db.transaction(storeName, mode);
        const store = transaction.objectStore(storeName);
        return { transaction, store };
    }

    // Topics Operations
    getTopics() {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('topics');
            const request = store.getAll();
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    addTopic(topic) {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('topics', 'readwrite');
            const request = store.add(topic);
            request.onsuccess = () => resolve(topic);
            request.onerror = () => reject(request.error);
        });
    }

    updateTopic(topic) {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('topics', 'readwrite');
            const request = store.put(topic);
            request.onsuccess = () => resolve(topic);
            request.onerror = () => reject(request.error);
        });
    }

    deleteTopic(topicId) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(['topics', 'stream_items'], 'readwrite');
            const topicsStore = tx.objectStore('topics');
            const streamStore = tx.objectStore('stream_items');
            
            // Delete root topic
            topicsStore.delete(topicId);
            
            // Delete stream items for root topic
            const streamIndex = streamStore.index('topicId');
            const streamReq = streamIndex.openCursor(IDBKeyRange.only(topicId));
            streamReq.onsuccess = (e) => {
                const cursor = e.target.result;
                if (cursor) {
                    streamStore.delete(cursor.primaryKey);
                    cursor.continue();
                }
            };
            
            // Delete subtopics and their stream items
            const topicsReq = topicsStore.openCursor();
            topicsReq.onsuccess = (e) => {
                const cursor = e.target.result;
                if (cursor) {
                    const topic = cursor.value;
                    if (topic.parentId === topicId) {
                        // Delete subtopic
                        topicsStore.delete(topic.id);
                        // Delete its stream items
                        const subStreamReq = streamStore.index('topicId').openCursor(IDBKeyRange.only(topic.id));
                        subStreamReq.onsuccess = (se) => {
                            const scursor = se.target.result;
                            if (scursor) {
                                streamStore.delete(scursor.primaryKey);
                                scursor.continue();
                            }
                        };
                    }
                    cursor.continue();
                }
            };

            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    }

    // Stream Items Operations
    getStreamItems(topicId) {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('stream_items');
            const index = store.index('topicId');
            const request = index.getAll(IDBKeyRange.only(topicId));
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    getStreamItem(itemId) {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('stream_items');
            const request = store.get(itemId);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    addStreamItem(item) {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('stream_items', 'readwrite');
            const request = store.add(item);
            request.onsuccess = () => resolve(item);
            request.onerror = () => reject(request.error);
        });
    }

    updateStreamItem(item) {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('stream_items', 'readwrite');
            const request = store.put(item);
            request.onsuccess = () => resolve(item);
            request.onerror = () => reject(request.error);
        });
    }

    deleteStreamItem(itemId) {
        return new Promise((resolve, reject) => {
            const { store } = this._getTransaction('stream_items', 'readwrite');
            const request = store.delete(itemId);
            request.onsuccess = () => resolve();
            request.onerror = () => reject(request.error);
        });
    }

    // Backup
    async getAllData() {
        const topics = await this.getTopics();
        const streamTx = this.db.transaction('stream_items', 'readonly');
        const streamReq = streamTx.objectStore('stream_items').getAll();
        const streamItems = await new Promise((res) => { streamReq.onsuccess = () => res(streamReq.result); });
        
        return { topics, streamItems };
    }

    async importAllData(data) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(['topics', 'stream_items'], 'readwrite');
            tx.objectStore('topics').clear();
            tx.objectStore('stream_items').clear();
            
            if (data.topics) {
                const topicStore = tx.objectStore('topics');
                data.topics.forEach(t => topicStore.add(t));
            }
            if (data.streamItems) {
                const streamStore = tx.objectStore('stream_items');
                data.streamItems.forEach(item => streamStore.add(item));
            }
            // Fallback if importing legacy backup format
            if (data.notes && !data.streamItems) {
                const streamStore = tx.objectStore('stream_items');
                data.notes.forEach(note => {
                    streamStore.add({
                        id: note.id,
                        topicId: note.topicId,
                        type: note.type === 'theory' ? 'text' : note.type,
                        title: note.title,
                        content: note.content,
                        code: note.code,
                        link: note.link,
                        imageData: note.imageData,
                        tags: note.tags,
                        createdAt: note.createdAt
                    });
                });
            }

            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    }
}

const dbManager = new DatabaseManager();

// ==========================================================================
// APPLICATION STATE
// ==========================================================================
const state = {
    topics: [],
    currentStreamItems: [],
    activeTopicId: null,
    searchQuery: '',
    currentAttachments: [] // Array of { id, type, name, size, mimeType, data }
};

// ==========================================================================
// DEMO DATA (Structured Chronologically for lectures)
// ==========================================================================
const demoData = {
    topics: [
        {
            id: 'demo-course-1',
            title: 'Dirbtinio Intelekto Kursas',
            emoji: '🧠',
            description: 'Išsamios studijos apie LLM, promptinimą ir RAG architektūras.',
            createdAt: Date.now()
        },
        {
            id: 'demo-lecture-1',
            parentId: 'demo-course-1',
            title: '1 Paskaita: Įvadas ir Promptai',
            emoji: '💡',
            description: 'Pagrindiniai principai, LLM instrukcijų rašymas ir Zero-Shot vs Few-Shot metodai.',
            createdAt: Date.now() + 1000
        },
        {
            id: 'demo-lecture-2',
            parentId: 'demo-course-1',
            title: '2 Paskaita: RAG sistemos',
            emoji: '🔍',
            description: 'Vektorinės duomenų bazės, embeddings ir modelio papildymas savo duomenimis.',
            createdAt: Date.now() + 2000
        }
    ],
    streamItems: [
        // Lecture 1 items
        {
            id: 'item-1',
            topicId: 'demo-lecture-1',
            type: 'text',
            title: 'Kas yra LLM Haliucinacijos?',
            content: 'Modelis generuoja faktiškai neteisingą informaciją su dideliu užtikrintumu. \n\n**Kaip to išvengti?**\n- Suteikti aiškų kontekstą (System instructions).\n- Liepti modeliui pasakyti "aš nežinau", jeigu atsakymo nėra pateiktame tekste.',
            tags: ['haliucinacijos', 'svarbu'],
            createdAt: Date.now() - 50000
        },
        {
            id: 'item-2',
            topicId: 'demo-lecture-1',
            type: 'code',
            title: 'Few-Shot pavyzdžio šablonas',
            code: 'Nurodyk teksto toną:\n\nAtsiliepimas: "Greitas pristatymas, prekė gera." -> Tonas: Teigiamas\nAtsiliepimas: "Sugedo po dviejų dienų." -> Tonas: Neigiamas\nAtsiliepimas: "Gavau prekę šiandien." -> Tonas:',
            tags: ['few-shot', 'prompts'],
            createdAt: Date.now() - 40000
        },
        {
            id: 'item-3',
            topicId: 'demo-lecture-1',
            type: 'image',
            title: 'Promptavimo principų skaidrė',
            content: 'Dėstytojo parodytas pagrindinių taisyklių rinkinys iš skaidrės.',
            // Abstract green/blue gradient image placeholder (small)
            imageData: 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="600" height="300" viewBox="0 0 600 300"><defs><linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" style="stop-color:%231e293b;stop-opacity:1"/><stop offset="100%" style="stop-color:%230f172a;stop-opacity:1"/></linearGradient></defs><rect width="600" height="300" fill="url(%23g)"/><text x="50%" y="45%" dominant-baseline="middle" text-anchor="middle" fill="%2306b6d4" font-family="sans-serif" font-size="28" font-weight="bold">PROMPTING PRINCIPLES</text><text x="50%" y="60%" dominant-baseline="middle" text-anchor="middle" fill="%2394a3b8" font-family="sans-serif" font-size="16">1. Write clear and specific instructions</text><text x="50%" y="70%" dominant-baseline="middle" text-anchor="middle" fill="%2394a3b8" font-family="sans-serif" font-size="16">2. Give the model time to think (chain of thought)</text></svg>',
            tags: ['slide', 'screenshot'],
            createdAt: Date.now() - 30000
        },
        {
            id: 'item-4',
            topicId: 'demo-lecture-1',
            type: 'link',
            title: 'Naudingas resursas: Learn Prompting',
            link: 'https://learnprompting.org',
            content: 'Nemokamas ir labai išsamus promptavimo kursas pradedantiesiems ir pažengusiems.',
            tags: ['mokymasis', 'links'],
            createdAt: Date.now() - 20000
        },
        // Lecture 2 items
        {
            id: 'item-5',
            topicId: 'demo-lecture-2',
            type: 'text',
            title: 'Kodėl RAG geriau nei fine-tuning žinių atnaujinimui?',
            content: 'Fine-tuning keičia modelio elgesį/stilių, bet yra prastas būdas "įkalti" naujus faktus (jie gali būti pamiršti). RAG veikia kaip **atviros knygos egzaminas** – modelis ieško tikslaus fakto dokumente ir juo remiasi sugeneruodamas atsakymą.',
            tags: ['rag', 'fine-tuning'],
            createdAt: Date.now() - 10000
        }
    ]
};

// ==========================================================================
// UI DOM ELEMENTS
// ==========================================================================
const dom = {
    // Navigation & Global
    topicsList: document.getElementById('topics-list'),
    searchInput: document.getElementById('search-input'),
    clearSearchBtn: document.getElementById('clear-search-btn'),
    newTopicBtn: document.getElementById('new-topic-btn'),
    exportBtn: document.getElementById('export-btn'),
    importBtn: document.getElementById('import-btn'),
    importFileInput: document.getElementById('import-file-input'),

    // Views
    welcomeView: document.getElementById('welcome-view'),
    topicView: document.getElementById('topic-view'),
    welcomeNewTopicBtn: document.getElementById('welcome-new-topic-btn'),
    quickDemoBtn: document.getElementById('quick-demo-btn'),

    // Active Topic Info
    topicIconDisplay: document.getElementById('topic-icon-display'),
    topicTitleDisplay: document.getElementById('topic-title-display'),
    topicDescDisplay: document.getElementById('topic-desc-display'),
    editTopicBtn: document.getElementById('edit-topic-btn'),
    deleteTopicBtn: document.getElementById('delete-topic-btn'),

    // Quick Capture
    quickCaptureBox: document.getElementById('quick-capture-box'),
    captureInput: document.getElementById('capture-input'),
    captureAttachmentsPreview: document.getElementById('capture-attachments-preview'),
    captureTitle: document.getElementById('capture-title'),
    captureTags: document.getElementById('capture-tags'),
    uploadImageTrigger: document.getElementById('upload-image-trigger'),
    captureFileInput: document.getElementById('capture-file-input'),
    saveCaptureBtn: document.getElementById('save-capture-btn'),

    // Stream List
    streamCountBadge: document.getElementById('stream-count-badge'),
    streamContainer: document.getElementById('stream-container'),

    // Modals
    topicModal: document.getElementById('topic-modal'),
    topicForm: document.getElementById('topic-form'),
    topicModalTitle: document.getElementById('topic-modal-title'),
    topicModalId: document.getElementById('topic-modal-id'),
    topicModalParentId: document.getElementById('topic-modal-parent-id'),
    topicTitleInput: document.getElementById('topic-title'),
    topicEmojiInput: document.getElementById('topic-emoji'),
    topicEmojiPickerBtn: document.getElementById('topic-emoji-picker-btn'),
    topicEmojiDropdown: document.getElementById('topic-emoji-dropdown'),
    topicDescInput: document.getElementById('topic-desc'),
    
    // Subtopics & Dashboard
    subtopicContentWrapper: document.getElementById('subtopic-content-wrapper'),
    rootDashboardWrapper: document.getElementById('root-dashboard-wrapper'),
    dashboardNewSubtopicBtn: document.getElementById('dashboard-new-subtopic-btn'),
    dashboardSubtopicsGrid: document.getElementById('dashboard-subtopics-grid'),

    editNoteModal: document.getElementById('edit-note-modal'),
    editNoteForm: document.getElementById('edit-note-form'),
    editNoteId: document.getElementById('edit-note-id'),
    editNoteType: document.getElementById('edit-note-type'),
    editNoteTitle: document.getElementById('edit-note-title'),
    editNoteContent: document.getElementById('edit-note-content'),
    editNoteEmojiPickerBtn: document.getElementById('edit-note-emoji-picker-btn'),
    editNoteEmojiDropdown: document.getElementById('edit-note-emoji-dropdown'),
    editNoteImageGroup: document.getElementById('edit-note-image-group'),
    editNoteImagePreview: document.getElementById('edit-note-image-preview'),
    editNoteTags: document.getElementById('edit-note-tags'),

    // Image Viewer Modal
    imageViewerModal: document.getElementById('image-viewer-modal'),
    viewerImg: document.getElementById('viewer-img'),
    viewerCaption: document.getElementById('viewer-caption'),
    closeViewerBtn: document.querySelector('.close-viewer-btn'),
    
    // Toolbar & Emojis
    emojiPickerBtn: document.getElementById('emoji-picker-btn'),
    emojiDropdown: document.getElementById('emoji-dropdown'),
    ocrExtractBtn: document.getElementById('ocr-extract-btn')
};

// ==========================================================================
// APPLICATION INITIALIZATION & CORE FUNCTIONS
// ==========================================================================
document.addEventListener('DOMContentLoaded', async () => {
    try {
        if (typeof pdfjsLib !== 'undefined') {
            pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.4.120/pdf.worker.min.js';
        }
        await dbManager.init();
        await loadAppState();
        registerEventListeners();
        lucide.createIcons();
    } catch (e) {
        console.error('Klaida kraunant aplikaciją:', e);
        alert('Nepavyko paleisti duomenų bazės. Patikrinkite naršyklės nustatymus.');
    }
});

// Load all items from DB into state
async function loadAppState() {
    state.topics = await dbManager.getTopics();
    
    // Sort topics by creation time
    state.topics.sort((a, b) => a.createdAt - b.createdAt);
    
    renderTopicsList();
    
    if (state.activeTopicId) {
        await selectTopic(state.activeTopicId);
    } else {
        showWelcomeView();
    }
}

// Render the sidebar topics list
function renderTopicsList() {
    dom.topicsList.innerHTML = '';
    
    let filteredTopics = state.topics;
    
    // Filter if search query is present
    if (state.searchQuery.trim() !== '') {
        const query = state.searchQuery.toLowerCase();
        filteredTopics = state.topics.filter(topic => 
            topic.title.toLowerCase().includes(query) || 
            (topic.description && topic.description.toLowerCase().includes(query))
        );
    }

    const rootTopics = filteredTopics.filter(t => !t.parentId);

    if (rootTopics.length === 0) {
        dom.topicsList.innerHTML = `<li class="tip-text" style="padding: 1rem; text-align: center;">Nėra temų</li>`;
        return;
    }

    rootTopics.forEach(rootTopic => {
        const groupLi = document.createElement('li');
        groupLi.className = 'topic-group';

        const subtopics = filteredTopics.filter(t => t.parentId === rootTopic.id);

        const rootItemDiv = document.createElement('div');
        rootItemDiv.className = `topic-item root-topic ${state.activeTopicId === rootTopic.id ? 'active' : ''}`;
        rootItemDiv.dataset.id = rootTopic.id;

        // Determine if this topic group should be expanded (if active topic is either the root topic or one of its subtopics)
        const isActiveInGroup = state.activeTopicId === rootTopic.id || subtopics.some(sub => sub.id === state.activeTopicId);
        
        const hasSubtopics = subtopics.length > 0;
        const caretHtml = hasSubtopics ? 
            `<i data-lucide="chevron-down" class="toggle-subtopics ${isActiveInGroup ? '' : 'collapsed'}"></i>` : 
            `<span style="width:14px; display:inline-block; flex-shrink:0;"></span>`;

        rootItemDiv.innerHTML = `
            ${caretHtml}
            <div class="topic-item-left" style="flex: 1; margin-left: 0.25rem;">
                <span class="topic-item-emoji">${rootTopic.emoji || '🧠'}</span>
                <span class="topic-item-name">${escapeHtml(rootTopic.title)}</span>
            </div>
            <span class="topic-item-badge">${subtopics.length} pot.</span>
            <button class="add-subtopic-btn" title="Pridėti potemę">
                <i data-lucide="plus"></i>
            </button>
        `;

        // Bind clicks
        rootItemDiv.addEventListener('click', (e) => {
            if (e.target.closest('.add-subtopic-btn') || e.target.closest('.toggle-subtopics')) return;
            selectTopic(rootTopic.id);
        });

        rootItemDiv.querySelector('.add-subtopic-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            openAddSubtopicModal(rootTopic.id);
        });

        if (hasSubtopics) {
            rootItemDiv.querySelector('.toggle-subtopics').addEventListener('click', (e) => {
                e.stopPropagation();
                const subList = groupLi.querySelector('.subtopics-list');
                const isCollapsed = subList.classList.toggle('hidden');
                e.target.closest('.toggle-subtopics').classList.toggle('collapsed', isCollapsed);
            });
        }

        groupLi.appendChild(rootItemDiv);

        if (hasSubtopics) {
            const subListUl = document.createElement('ul');
            subListUl.className = `subtopics-list ${isActiveInGroup ? '' : 'hidden'}`;

            subtopics.forEach(sub => {
                const subLi = document.createElement('li');
                subLi.className = `topic-item sub-topic ${state.activeTopicId === sub.id ? 'active' : ''}`;
                subLi.dataset.id = sub.id;

                dbManager.getStreamItems(sub.id).then(items => {
                    const badge = subLi.querySelector('.topic-item-badge');
                    if (badge) badge.textContent = `${items.length}`;
                });

                subLi.innerHTML = `
                    <div class="topic-item-left">
                        <span class="topic-item-emoji">${sub.emoji || '💡'}</span>
                        <span class="topic-item-name">${escapeHtml(sub.title)}</span>
                    </div>
                    <span class="topic-item-badge">0</span>
                `;

                subLi.addEventListener('click', () => selectTopic(sub.id));
                subListUl.appendChild(subLi);
            });

            groupLi.appendChild(subListUl);
        }

        dom.topicsList.appendChild(groupLi);
    });

    lucide.createIcons();
}

function openAddSubtopicModal(parentId) {
    dom.topicForm.reset();
    dom.topicModalId.value = '';
    dom.topicModalParentId.value = parentId;
    dom.topicTitleInput.value = '';
    dom.topicTitleInput.placeholder = 'Pvz., 1 Paskaita: Promptai';
    dom.topicEmojiInput.value = '💡';
    dom.topicDescInput.value = '';
    
    const parentTopic = state.topics.find(t => t.id === parentId);
    dom.topicModalTitle.textContent = `Nauja potemė (paskaita) kursui „${parentTopic.title}“`;
    openModal(dom.topicModal);
}

// Select a Topic and display details
async function selectTopic(topicId) {
    state.activeTopicId = topicId;
    
    // Highlight active topic in list
    document.querySelectorAll('.topic-item').forEach(item => {
        item.classList.toggle('active', item.dataset.id === topicId);
    });

    const topic = state.topics.find(t => t.id === topicId);
    if (!topic) {
        showWelcomeView();
        return;
    }

    if (!topic.parentId) {
        // Root Topic -> Show Dashboard
        dom.subtopicContentWrapper.classList.add('hidden');
        dom.rootDashboardWrapper.classList.remove('hidden');
        
        // Update Header
        dom.topicIconDisplay.textContent = topic.emoji || '🧠';
        dom.topicTitleDisplay.textContent = topic.title;
        
        const hasDesc1 = topic.description && topic.description.trim() && 
                         !topic.description.includes('Aprašymo nėra') && 
                         topic.description !== 'Aprašymo nėra... Redaguokite temą, kad pridėtumėte.';
        if (hasDesc1) {
            dom.topicDescDisplay.textContent = topic.description;
            dom.topicDescDisplay.classList.remove('hidden');
        } else {
            dom.topicDescDisplay.textContent = '';
            dom.topicDescDisplay.classList.add('hidden');
        }
        
        // Render Dashboard Subtopics Grid
        renderDashboardSubtopics(topic.id);
    } else {
        // Subtopic -> Show Stream and Quick Capture
        dom.rootDashboardWrapper.classList.add('hidden');
        dom.subtopicContentWrapper.classList.remove('hidden');
        
        // Find parent
        const parent = state.topics.find(t => t.id === topic.parentId);
        const parentTitle = parent ? parent.title : '';
        
        // Update Header with Breadcrumbs
        dom.topicIconDisplay.textContent = topic.emoji || '💡';
        dom.topicTitleDisplay.innerHTML = `<span class="parent-topic-title">${escapeHtml(parentTitle)}</span><span class="breadcrumb-separator">></span>${escapeHtml(topic.title)}`;
        
        const hasDesc2 = topic.description && topic.description.trim() && 
                         !topic.description.includes('Aprašymo nėra') && 
                         topic.description !== 'Aprašymo nėra... Redaguokite temą, kad pridėtumėte.';
        if (hasDesc2) {
            dom.topicDescDisplay.textContent = topic.description;
            dom.topicDescDisplay.classList.remove('hidden');
        } else {
            dom.topicDescDisplay.textContent = '';
            dom.topicDescDisplay.classList.add('hidden');
        }
        
        // Load stream items for this subtopic
        state.currentStreamItems = await dbManager.getStreamItems(topicId);
        state.currentStreamItems.sort((a, b) => a.createdAt - b.createdAt);
        
        renderStreamFlow();
    }

    // Show View
    dom.welcomeView.classList.add('hidden');
    dom.topicView.classList.remove('hidden');
}

function renderDashboardSubtopics(parentId) {
    dom.dashboardSubtopicsGrid.innerHTML = '';
    
    const subtopics = state.topics.filter(t => t.parentId === parentId);
    
    if (subtopics.length === 0) {
        dom.dashboardSubtopicsGrid.innerHTML = `
            <div class="tip-text" style="grid-column: 1/-1; padding: 3rem; text-align: center; border: 1px dashed var(--border-color); border-radius: var(--radius-md); width:100%;">
                Ši tema dar neturi potemų. Paspauskite mygtuką viršuje, kad pridėtumėte pirmąją paskaitą ar potemę!
            </div>
        `;
        return;
    }
    
    subtopics.forEach(sub => {
        const card = document.createElement('div');
        card.className = 'subtopic-card';
        
        dbManager.getStreamItems(sub.id).then(items => {
            const countBadge = card.querySelector('.subtopic-card-count');
            if (countBadge) countBadge.textContent = `${items.length} įrašų`;
        });
        
        card.innerHTML = `
            <div class="subtopic-card-top">
                <span class="subtopic-card-emoji">${sub.emoji || '💡'}</span>
                <div>
                    <h3 class="subtopic-card-title">${escapeHtml(sub.title)}</h3>
                    ${sub.description && sub.description.trim() && !sub.description.includes('Aprašymo nėra') ? `<p class="subtopic-card-desc">${escapeHtml(sub.description)}</p>` : ''}
                </div>
            </div>
            <div class="subtopic-card-footer">
                <span class="subtopic-card-count">0 įrašų</span>
            </div>
        `;
        
        card.addEventListener('click', () => selectTopic(sub.id));
        dom.dashboardSubtopicsGrid.appendChild(card);
    });
}

function showWelcomeView() {
    state.activeTopicId = null;
    dom.topicView.classList.add('hidden');
    dom.welcomeView.classList.remove('hidden');
    
    document.querySelectorAll('.topic-item').forEach(item => {
        item.classList.remove('active');
    });
}

// ==========================================================================
// RENDER STREAM FLOW (Timeline List)
// ==========================================================================
function renderStreamFlow() {
    dom.streamContainer.innerHTML = '';
    
    let filteredItems = state.currentStreamItems;
    
    // Search query filter (if active query)
    if (state.searchQuery.trim() !== '') {
        const query = state.searchQuery.toLowerCase();
        filteredItems = state.currentStreamItems.filter(item => 
            (item.title && item.title.toLowerCase().includes(query)) || 
            (item.content && item.content.toLowerCase().includes(query)) ||
            (item.code && item.code.toLowerCase().includes(query)) ||
            (item.link && item.link.toLowerCase().includes(query)) ||
            (item.tags && item.tags.some(t => t.toLowerCase().includes(query)))
        );
    }

    // Update stream count badge
    dom.streamCountBadge.textContent = `${filteredItems.length} įrašų`;

    if (filteredItems.length === 0) {
        dom.streamContainer.innerHTML = `
            <div class="tip-text" style="padding: 3rem; text-align: center; border: 1px dashed var(--border-color); border-radius: var(--radius-md); margin-top: 1rem;">
                Šiame sraute įrašų nėra. Įrašykite pastabą arba įklijuokite ekrano nuotrauką viršuje!
            </div>
        `;
        return;
    }

    filteredItems.forEach(item => {
        const card = document.createElement('div');
        card.className = `stream-card type-${item.type}`;
        card.dataset.id = item.id;
        
        // Time string formatting
        const timeStr = new Date(item.createdAt).toLocaleTimeString('lt-LT', { hour: '2-digit', minute: '2-digit' });
        const dateStr = new Date(item.createdAt).toLocaleDateString('lt-LT', { month: 'short', day: 'numeric' });
        const displayTime = `${dateStr}, ${timeStr}`;

        // Build HTML depending on card type
        let bodyHtml = '';
        let typeLabel = 'Pastaba';
        
        // Helper: render markdown but preserve deliberate blank lines as visual spacing
        const renderMd = (text) => {
            if (!text) return '';
            // Extra blank lines (3+ newlines) become explicit spacer elements before markdown parsing
            const processed = text
                .replace(/\n{3,}/g, (match) => {
                    // One blank line = paragraph break (\n\n). Extra ones = extra spacer divs
                    const extras = Math.floor(match.length / 2) - 1;
                    return '\n\n' + '<div style="height:0.75rem"></div>\n\n'.repeat(extras);
                });
            return marked.parse(processed);
        };

        if (item.type === 'text') {
            bodyHtml = `<div class="card-body">${renderMd(item.content || '')}</div>`;
            typeLabel = 'Tekstas';
        } else if (item.type === 'code') {
            bodyHtml = `
                ${item.content ? `<div class="card-body" style="margin-bottom: 0.5rem;">${renderMd(item.content)}</div>` : ''}
                <div class="code-wrapper">
                    <pre><code>${escapeHtml(item.code || '')}</code></pre>
                    <button class="copy-code-btn" data-code="${escapeAttribute(item.code || '')}">
                        <i data-lucide="clipboard"></i> Kopijuoti
                    </button>
                </div>
            `;
            typeLabel = 'Prompt / Kodas';
        } else if (item.type === 'link') {
            // Simplify URL showing
            let linkDomain = 'Nuoroda';
            try {
                linkDomain = new URL(item.link).hostname;
            } catch (e) {}

            bodyHtml = `
                ${item.content ? `<div class="card-body" style="margin-bottom: 0.5rem;">${renderMd(item.content)}</div>` : ''}
                <a href="${escapeAttribute(item.link)}" target="_blank" rel="noopener noreferrer" class="bookmark-container">
                    <div class="bookmark-icon-block">
                        <i data-lucide="link"></i>
                    </div>
                    <div class="bookmark-info">
                        <div class="bookmark-title">${escapeHtml(item.title || 'Atidaryti nuorodą')}</div>
                        <div class="bookmark-desc">${escapeHtml(item.link)}</div>
                        <div class="bookmark-url">${escapeHtml(linkDomain)} <i data-lucide="external-link" style="width: 10px; height: 10px;"></i></div>
                    </div>
                </a>
            `;
            typeLabel = 'Nuoroda';
        } else if (item.type === 'image') {
            bodyHtml = `
                ${item.content ? `<div class="card-body" style="margin-bottom: 0.5rem;">${renderMd(item.content)}</div>` : ''}
                <div class="stream-img-wrapper">
                    <img src="${item.imageData}" alt="${escapeHtml(item.title || 'Nuotrauka')}" loading="lazy">
                </div>
            `;
            typeLabel = 'Nuotrauka';
        } else if (item.type === 'file') {
            let fileIcon = 'file';
            const isPdfFile = item.fileType && item.fileType.includes('pdf');
            if (isPdfFile) {
                fileIcon = 'file-text';
            } else if (item.fileType && (item.fileType.includes('zip') || item.fileType.includes('rar') || item.fileType.includes('tar'))) {
                fileIcon = 'file-archive';
            }
            
            const extractBtnHtml = isPdfFile ? `
                <button class="btn btn-secondary btn-sm extract-pdf-btn" data-item-id="${item.id}" style="padding:0.4rem 0.8rem; font-size:0.75rem; display:inline-flex; align-items:center; gap:0.25rem;" title="Ištraukti tekstą iš PDF ir paslėpti failą">
                    <i data-lucide="scan-text"></i> Ištraukti tekstą
                </button>
            ` : '';
            
            bodyHtml = `
                ${item.content ? `<div class="card-body" style="margin-bottom: 0.5rem;">${renderMd(item.content)}</div>` : ''}
                <div style="display:flex; align-items:center; gap:0.75rem; background:rgba(0,0,0,0.15); border:1px solid rgba(255,255,255,0.04); border-radius:var(--radius-sm); padding:0.75rem 1rem; margin-top:0.5rem;">
                    <div style="width:40px; height:40px; border-radius:6px; background:rgba(59,130,246,0.06); display:flex; align-items:center; justify-content:center; flex-shrink:0;">
                        <i data-lucide="${fileIcon}" style="color:var(--color-file); width:18px; height:18px;"></i>
                    </div>
                    <div style="flex:1; overflow:hidden;">
                        <div style="font-weight:700; font-size:0.9rem; color:var(--text-primary); white-space:nowrap; text-overflow:ellipsis; overflow:hidden;">${escapeHtml(item.title)}</div>
                        <div style="font-size:0.75rem; color:var(--text-muted); margin-top:0.15rem;">${escapeHtml(formatBytes(item.fileSize))}</div>
                    </div>
                    <div style="display:flex; gap:0.5rem; margin-left:auto; align-items:center;">
                        ${extractBtnHtml}
                        <a href="${item.fileData}" download="${escapeAttribute(item.title)}" class="btn btn-secondary btn-sm" style="padding:0.4rem 0.8rem; font-size:0.75rem; display:inline-flex; align-items:center; gap:0.25rem;">
                            <i data-lucide="download"></i> Atsisiūsti
                        </a>
                    </div>
                </div>
            `;
            typeLabel = 'Failas';
        }

        // Build attachments HTML if present
        // For 'file' type items, skip file attachments (main file is already rendered in bodyHtml above)
        let attachmentsHtml = '';
        if (item.attachments && item.attachments.length > 0) {
            const images = item.attachments.filter(a => a.type === 'image');
            // Don't show file attachments for file-type items to avoid showing same file twice
            const files = item.type === 'file' ? [] : item.attachments.filter(a => a.type === 'file');
            
            let imagesGridHtml = '';
            if (images.length > 0) {
                const colsClass = images.length === 1 ? 'cols-1' : (images.length === 2 ? 'cols-2' : 'cols-3');
                imagesGridHtml = `
                    <div class="stream-card-images-grid ${colsClass}">
                        ${images.map(img => `
                            <div class="stream-card-image-wrapper" data-src="${img.data}" data-title="${escapeAttribute(img.name)}">
                                <img src="${img.data}" alt="${escapeHtml(img.name)}" loading="lazy">
                            </div>
                        `).join('')}
                    </div>
                `;
            }
            
            let filesListHtml = '';
            if (files.length > 0) {
                filesListHtml = `
                    <div class="stream-card-files-list">
                        ${files.map(file => {
                            let fileIcon = 'file';
                            if (file.mimeType && file.mimeType.includes('pdf')) {
                                fileIcon = 'file-text';
                            } else if (file.mimeType && (file.mimeType.includes('zip') || file.mimeType.includes('rar') || file.mimeType.includes('tar'))) {
                                fileIcon = 'file-archive';
                            }
                            return `
                                <div class="stream-card-file-item" data-att-id="${file.id}" data-item-id="${item.id}">
                                    <i data-lucide="${fileIcon}" class="stream-card-file-icon"></i>
                                    <div class="stream-card-file-details">
                                        <div class="stream-card-file-name">${escapeHtml(file.name)}</div>
                                        <div class="stream-card-file-size">${escapeHtml(formatBytes(file.size))}</div>
                                    </div>
                                    <div style="display:flex;gap:0.4rem;margin-left:auto;align-items:center;">
                                        <a href="${file.data}" download="${escapeAttribute(file.name)}" class="btn btn-secondary btn-sm" style="padding: 0.4rem 0.8rem; font-size: 0.75rem;">
                                            <i data-lucide="download"></i> Atsisiųsti
                                        </a>
                                        <button type="button" class="btn-card-action btn-card-danger remove-attachment-btn" data-att-id="${file.id}" data-item-id="${item.id}" title="Pašalinti priedą" style="opacity:1;flex-shrink:0;">
                                            <i data-lucide="x"></i>
                                        </button>
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                `;
            }
            
            attachmentsHtml = `
                <div class="stream-card-attachments">
                    ${imagesGridHtml}
                    ${filesListHtml}
                </div>
            `;
        }

        card.innerHTML = `
            <div class="stream-card-header">
                ${item.title ? `
                <div class="stream-card-meta">
                    <span class="card-title">${escapeHtml(item.title)}</span>
                </div>
                ` : ''}
                <div class="card-actions">
                    <button class="btn-card-action edit-item-btn" title="Redaguoti">
                        <i data-lucide="edit-2"></i>
                    </button>
                    <button class="btn-card-action btn-card-danger delete-item-btn" title="Ištrinti">
                        <i data-lucide="trash-2"></i>
                    </button>
                </div>
            </div>
            ${bodyHtml}
            ${attachmentsHtml}
            ${item.tags && item.tags.length > 0 ? `
                <div class="card-footer">
                    ${item.tags.map(t => `<span class="tag">#${escapeHtml(t)}</span>`).join('')}
                </div>
            ` : ''}
        `;

        // Bind events
        card.querySelector('.edit-item-btn').addEventListener('click', (e) => { e.stopPropagation(); openEditItemModal(item.id); });
        card.querySelector('.delete-item-btn').addEventListener('click', (e) => { e.stopPropagation(); deleteStreamItem(item.id); });
        
        // Copy Code Button handler
        if (item.type === 'code') {
            const copyBtn = card.querySelector('.copy-code-btn');
            copyBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                navigator.clipboard.writeText(item.code).then(() => {
                    copyBtn.classList.add('copied');
                    copyBtn.innerHTML = `<i data-lucide="check"></i> Nukopijuota!`;
                    lucide.createIcons();
                    setTimeout(() => {
                        copyBtn.classList.remove('copied');
                        copyBtn.innerHTML = `<i data-lucide="clipboard"></i> Kopijuoti`;
                        lucide.createIcons();
                    }, 2000);
                });
            });
        }

        // Image Viewer Zoom handler (legacy)
        if (item.type === 'image' && card.querySelector('.stream-img-wrapper')) {
            const imgWrapper = card.querySelector('.stream-img-wrapper');
            imgWrapper.addEventListener('click', () => {
                openImageViewer(item.imageData, item.title || 'Paskaitos ekrano nuotrauka', item.content || '');
            });
        }

        // Multi-image viewer Zoom handler
        card.querySelectorAll('.stream-card-image-wrapper').forEach(imgWrapper => {
            imgWrapper.addEventListener('click', () => {
                const src = imgWrapper.dataset.src;
                const title = imgWrapper.dataset.title;
                openImageViewer(src, title, item.content || '');
            });
        });

        // PDF Extract Text button on saved stream cards
        const extractPdfBtn = card.querySelector('.extract-pdf-btn');
        if (extractPdfBtn) {
            extractPdfBtn.addEventListener('click', async (e) => {
                e.stopPropagation();
                await extractAndHidePdfOnCard(item, extractPdfBtn);
            });
        }

        // Remove individual file attachment directly from card (X button)
        card.querySelectorAll('.remove-attachment-btn').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const attId = btn.dataset.attId;
                const itemId = btn.dataset.itemId;
                const fresh = await dbManager.getStreamItem(itemId);
                if (!fresh) return;
                fresh.attachments = (fresh.attachments || []).filter(a => a.id !== attId);
                fresh.updatedAt = Date.now();
                await dbManager.updateStreamItem(fresh);
                // Re-render stream
                state.currentStreamItems = await dbManager.getStreamItems(state.activeTopicId);
                state.currentStreamItems.sort((a, b) => a.createdAt - b.createdAt);
                renderStreamFlow();
            });
        });

        dom.streamContainer.appendChild(card);
    });

    lucide.createIcons();
}

// Scroll stream list to the bottom (helpful when adding items so it acts like a feed)
function scrollStreamToBottom() {
    setTimeout(() => {
        dom.streamContainer.scrollTop = dom.streamContainer.scrollHeight;
    }, 50);
}

// ==========================================================================
// BUSINESS LOGIC ACTIONS (QUICK CAPTURE)
// ==========================================================================

// Parse input string to detect type: link, code, or standard text
function detectContentType(text) {
    const trimmed = text.trim();
    const urlRegex = /^(https?:\/\/[^\s]+)$/i;
    
    // Check if it is a single URL link
    if (urlRegex.test(trimmed)) {
        return 'link';
    }
    
    // Check for explicit code markers (triple backticks)
    if (trimmed.startsWith('```') && trimmed.endsWith('```')) {
        return 'code';
    }

    // Code structures heuristics (multiline + code indicators)
    const codeIndicators = ['{', '}', 'def ', 'import ', 'const ', 'let ', 'function', 'class ', 'print(', 'console.log', '<html>', 'select * from', 'pip install'];
    const lines = trimmed.split('\n');
    if (lines.length > 2) {
        const hasIndicators = codeIndicators.some(ind => trimmed.includes(ind));
        if (hasIndicators) return 'code';
    }
    
    return 'text';
}

// Save Quick Capture Box Content
// Save Quick Capture Box Content
async function saveQuickCapture() {
    const text = dom.captureInput.value.trim();
    const title = dom.captureTitle.value.trim();
    const tagsText = dom.captureTags.value.trim();
    const tags = tagsText ? tagsText.split(',').map(t => t.trim()).filter(t => t.length > 0) : [];
    
    if (!state.activeTopicId) return;

    // Must have either text or attachments
    if (!text && state.currentAttachments.length === 0) {
        return;
    }

    let type = 'text';
    let content = text;
    let code = '';
    let link = '';
    let finalTitle = title;

    if (state.currentAttachments.length > 0) {
        const firstAtt = state.currentAttachments[0];
        type = firstAtt.type === 'image' ? 'image' : 'file';
        
        if (!finalTitle) {
            finalTitle = firstAtt.name;
        }
    } else {
        // Automatically determine type
        type = detectContentType(text);
        
        if (type === 'link') {
            link = text;
            content = '';
            if (!finalTitle) finalTitle = 'Nuoroda';
        } else if (type === 'code') {
            // Strip markdown backticks if wrapped
            if (text.startsWith('```') && text.endsWith('```')) {
                const lines = text.split('\n');
                lines.shift(); // remove first line (```)
                lines.pop();   // remove last line (```)
                code = lines.join('\n');
            } else {
                code = text;
            }
            content = '';
            if (!finalTitle) finalTitle = 'Promptas / Kodas';
        } else {
            // Text pastaba
            content = text;
        }
    }

    // Map current attachments to DB schema format
    // For file/image type items: the FIRST attachment IS the item itself (stored in fileData/imageData)
    // so we only store EXTRA attachments (beyond the first) to avoid duplication
    let attachments = [];
    let fileData = undefined;
    let fileType = undefined;
    let fileSize = undefined;
    let imageData = undefined;

    if (state.currentAttachments.length > 0) {
        const firstAtt = state.currentAttachments[0];
        if (type === 'image') {
            imageData = firstAtt.data;
            // Extra attachments after the first image
            attachments = state.currentAttachments.slice(1).map(att => ({
                id: att.id, type: att.type, name: att.name,
                size: att.size, mimeType: att.mimeType, data: att.data
            }));
        } else if (type === 'file') {
            fileData = firstAtt.data;
            fileType = firstAtt.mimeType;
            fileSize = firstAtt.size;
            // Extra attachments after the first file
            attachments = state.currentAttachments.slice(1).map(att => ({
                id: att.id, type: att.type, name: att.name,
                size: att.size, mimeType: att.mimeType, data: att.data
            }));
        } else {
            // Text type with images/files attached
            attachments = state.currentAttachments.map(att => ({
                id: att.id, type: att.type, name: att.name,
                size: att.size, mimeType: att.mimeType, data: att.data
            }));
        }
    }

    const newItem = {
        id: generateId(),
        topicId: state.activeTopicId,
        type,
        title: finalTitle,
        content,
        code,
        link,
        attachments,
        fileData,
        fileType,
        fileSize,
        imageData,
        tags,
        createdAt: Date.now()
    };

    await dbManager.addStreamItem(newItem);

    // Reset capture box UI
    dom.captureInput.value = '';
    dom.captureTitle.value = '';
    dom.captureTags.value = '';
    clearAllAttachments();

    // Reload active stream & display
    state.currentStreamItems = await dbManager.getStreamItems(state.activeTopicId);
    state.currentStreamItems.sort((a, b) => a.createdAt - b.createdAt);
    
    renderStreamFlow();
    scrollStreamToBottom();
    
    // Update badge in sidebar topic item
    renderTopicsList();
}

// Universal Attachments Render & Management
function renderCaptureAttachments() {
    const container = dom.captureAttachmentsPreview;
    container.innerHTML = '';
    
    if (state.currentAttachments.length === 0) {
        container.classList.add('hidden');
        return;
    }
    
    container.classList.remove('hidden');
    
    state.currentAttachments.forEach(att => {
        const itemDiv = document.createElement('div');
        itemDiv.className = 'attachment-preview-item';
        
        let visualHtml = '';
        if (att.type === 'image') {
            visualHtml = `<img src="${att.data}" class="attachment-preview-thumb" alt="Nuotrauka">`;
        } else {
            let fileIcon = 'file';
            if (att.mimeType && att.mimeType.includes('pdf')) {
                fileIcon = 'file-text';
            } else if (att.mimeType && (att.mimeType.includes('zip') || att.mimeType.includes('rar') || att.mimeType.includes('tar'))) {
                fileIcon = 'file-archive';
            }
            visualHtml = `<i data-lucide="${fileIcon}" class="attachment-preview-icon"></i>`;
        }
        
        const isPdf = att.name.toLowerCase().endsWith('.pdf') || (att.mimeType && att.mimeType.includes('pdf'));
        const isImage = att.type === 'image';
        
        let actionBtnHtml = '';
        if (isImage) {
            actionBtnHtml = `
                <button type="button" class="btn btn-secondary btn-sm btn-preview-ocr" data-id="${att.id}" title="Nuskaityti tekstą iš nuotraukos (OCR)">
                    <i data-lucide="scan-text"></i> Ištraukti tekstą
                </button>
            `;
        } else if (isPdf) {
            actionBtnHtml = `
                <button type="button" class="btn btn-secondary btn-sm btn-preview-ocr" data-id="${att.id}" title="Nuskaityti tekstą iš PDF (OCR)">
                    <i data-lucide="scan-text"></i> Ištraukti tekstą
                </button>
            `;
        }
        
        itemDiv.innerHTML = `
            <div class="attachment-preview-left">
                ${visualHtml}
                <div class="attachment-preview-details">
                    <span class="attachment-preview-name">${escapeHtml(att.name)}</span>
                    <span class="attachment-preview-size">${escapeHtml(formatBytes(att.size))}</span>
                </div>
            </div>
            <div class="attachment-preview-actions">
                ${actionBtnHtml}
                <button type="button" class="remove-img-btn remove-att-btn" data-id="${att.id}" title="Pašalinti" style="position: static;">
                    <i data-lucide="x"></i>
                </button>
            </div>
        `;
        
        // Bind actions
        itemDiv.querySelector('.remove-att-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            removeAttachment(att.id);
        });
        
        const ocrBtn = itemDiv.querySelector('.btn-preview-ocr');
        if (ocrBtn) {
            ocrBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                if (isImage) {
                    runOcrForAttachment(att.id);
                } else if (isPdf) {
                    runPdfOcrForAttachment(att.id);
                }
            });
        }
        
        container.appendChild(itemDiv);
    });
    
    lucide.createIcons();
}

function removeAttachment(id) {
    state.currentAttachments = state.currentAttachments.filter(att => att.id !== id);
    renderCaptureAttachments();
}

function clearAllAttachments() {
    state.currentAttachments = [];
    renderCaptureAttachments();
}

// Helper to format file size bytes
function formatBytes(bytes, decimals = 1) {
    if (!bytes) return '0 B';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// Handle Paste globally and in textarea
window.addEventListener('paste', (e) => {
    if (!state.activeTopicId) return;

    const items = (e.clipboardData || e.originalEvent.clipboardData).items;
    
    for (let i = 0; i < items.length; i++) {
        if (items[i].type.indexOf('image') !== -1) {
            const file = items[i].getAsFile();
            const reader = new FileReader();
            
            reader.onload = (event) => {
                const dateStr = new Date().toLocaleTimeString('lt-LT');
                const newAttachment = {
                    id: generateId(),
                    type: 'image',
                    name: `Ekrano nuotrauka (${dateStr})`,
                    size: file.size,
                    mimeType: file.type,
                    data: event.target.result
                };
                
                state.currentAttachments.push(newAttachment);
                renderCaptureAttachments();
                dom.captureInput.focus();
            };
            
            reader.readAsDataURL(file);
            e.preventDefault();
            break;
        }
    }
});

// Image file upload manual trigger
dom.uploadImageTrigger.addEventListener('click', () => {
    dom.captureFileInput.click();
});

function handleSelectedFile(file) {
    if (!file) return;

    // 1. If it's a plain text file (txt, md, js, json, etc) -> parse directly into text
    if (file.type.match('text.*') || file.name.endsWith('.txt') || file.name.endsWith('.md') || file.name.endsWith('.json') || file.name.endsWith('.js')) {
        const reader = new FileReader();
        reader.onload = (event) => {
            const textContent = event.target.result.trim();
            const currentVal = dom.captureInput.value.trim();
            dom.captureInput.value = (currentVal ? currentVal + '\n\n' : '') + textContent;
            dom.captureInput.focus();
        };
        reader.readAsText(file);
        return;
    }

    // 2. Otherwise, treat as an attachment (image or generic file)
    const reader = new FileReader();
    reader.onload = (event) => {
        const isImage = file.type.match('image.*');
        const newAttachment = {
            id: generateId(),
            type: isImage ? 'image' : 'file',
            name: file.name,
            size: file.size,
            mimeType: file.type,
            data: event.target.result
        };
        
        state.currentAttachments.push(newAttachment);
        renderCaptureAttachments();
        dom.captureInput.focus();
    };
    reader.readAsDataURL(file);
}

dom.captureFileInput.addEventListener('change', (e) => {
    const files = e.target.files;
    if (files) {
        for (let i = 0; i < files.length; i++) {
            handleSelectedFile(files[i]);
        }
    }
    dom.captureFileInput.value = '';
});

// ==========================================================================
// BUSINESS LOGIC ACTIONS (TOPICS & STREAM EDITING)
// ==========================================================================

// Create/Edit Topic Submit
dom.topicForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const id = dom.topicModalId.value;
    const parentId = dom.topicModalParentId.value.trim() || undefined;
    const title = dom.topicTitleInput.value.trim();
    const emoji = dom.topicEmojiInput.value.trim() || '💡';
    const description = dom.topicDescInput.value.trim();

    if (!title) return;

    if (id) {
        // Edit Mode
        const topic = state.topics.find(t => t.id === id);
        if (topic) {
            topic.title = title;
            topic.emoji = emoji;
            topic.description = description;
            await dbManager.updateTopic(topic);
        }
    } else {
        // Create Mode
        const newTopic = {
            id: generateId(),
            parentId,
            title,
            emoji,
            description,
            createdAt: Date.now()
        };
        state.topics.push(newTopic);
        await dbManager.addTopic(newTopic);
        state.activeTopicId = newTopic.id;
    }

    closeModal(dom.topicModal);
    await loadAppState();
});

// Delete Topic
async function deleteTopic() {
    if (!state.activeTopicId) return;
    
    const topic = state.topics.find(t => t.id === state.activeTopicId);
    if (!topic) return;

    if (confirm(`Ar tikrai norite ištrinti temą „${topic.title}“ ir visus joje esančius srauto įrašus?`)) {
        await dbManager.deleteTopic(state.activeTopicId);
        state.activeTopicId = null;
        await loadAppState();
    }
}

// Edit Existing Item in Stream (Modal)
async function openEditItemModal(itemId) {
    const item = await dbManager.getStreamItem(itemId);
    if (!item) return;

    dom.editNoteId.value = item.id;
    dom.editNoteType.value = item.type;
    dom.editNoteTitle.value = item.title || '';
    dom.editNoteTags.value = (item.tags || []).join(', ');

    // Fill edit fields based on type
    if (item.type === 'code') {
        dom.editNoteContent.value = item.code || '';
    } else if (item.type === 'link') {
        dom.editNoteContent.value = item.link || '';
    } else {
        dom.editNoteContent.value = item.content || '';
    }

    // Toggle image group preview in modal
    if (item.type === 'image' && item.imageData) {
        dom.editNoteImagePreview.src = item.imageData;
        dom.editNoteImageGroup.classList.remove('hidden');
    } else {
        dom.editNoteImageGroup.classList.add('hidden');
        dom.editNoteImagePreview.src = '';
    }

    // Show attachments panel if item has saved file/image attachments
    const attachmentsGroup = document.getElementById('edit-note-attachments-group');
    const attachmentsList = document.getElementById('edit-note-attachments-list');
    const savedAttachments = (item.attachments || []).filter(a => a.type === 'file');
    if (savedAttachments.length > 0) {
        attachmentsGroup.classList.remove('hidden');
        attachmentsList.innerHTML = '';
        savedAttachments.forEach(att => {
            const row = document.createElement('div');
            row.style.cssText = 'display:flex; align-items:center; gap:0.5rem; padding:0.5rem 0.75rem; background:rgba(0,0,0,0.15); border:1px solid rgba(255,255,255,0.06); border-radius:6px;';
            row.innerHTML = `
                <i data-lucide="file-text" style="width:16px;height:16px;color:var(--color-file);flex-shrink:0;"></i>
                <span style="flex:1;font-size:0.85rem;color:var(--text-secondary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escapeHtml(att.name)}</span>
                <span style="font-size:0.75rem;color:var(--text-muted);flex-shrink:0;">${escapeHtml(formatBytes(att.size))}</span>
                <button type="button" class="btn-card-action btn-card-danger" data-att-id="${att.id}" title="Pašalinti priedą" style="opacity:1;">
                    <i data-lucide="x"></i>
                </button>
            `;
            row.querySelector('button').addEventListener('click', async () => {
                const fresh = await dbManager.getStreamItem(item.id);
                if (!fresh) return;
                fresh.attachments = (fresh.attachments || []).filter(a => a.id !== att.id);
                await dbManager.updateStreamItem(fresh);
                row.remove();
                if (attachmentsList.children.length === 0) attachmentsGroup.classList.add('hidden');
                // Refresh stream view
                state.currentStreamItems = await dbManager.getStreamItems(state.activeTopicId);
                state.currentStreamItems.sort((a, b) => a.createdAt - b.createdAt);
                renderStreamFlow();
            });
            attachmentsList.appendChild(row);
        });
        lucide.createIcons();
    } else {
        attachmentsGroup.classList.add('hidden');
    }

    openModal(dom.editNoteModal);
}

// Submit Edit Item
dom.editNoteForm.addEventListener('submit', async (e) => {
    e.preventDefault();

    const id = dom.editNoteId.value;
    const type = dom.editNoteType.value;
    const title = dom.editNoteTitle.value.trim();
    const rawContent = dom.editNoteContent.value.trim();
    const tagsText = dom.editNoteTags.value.trim();
    const tags = tagsText ? tagsText.split(',').map(t => t.trim()).filter(t => t.length > 0) : [];

    if (!id || !state.activeTopicId) return;

    const existing = await dbManager.getStreamItem(id);
    if (!existing) return;

    // Apply values depending on type
    existing.title = title;
    existing.tags = tags;
    existing.updatedAt = Date.now();

    if (type === 'code') {
        existing.code = rawContent;
    } else if (type === 'link') {
        existing.link = rawContent;
    } else {
        existing.content = rawContent;
    }

    await dbManager.updateStreamItem(existing);
    closeModal(dom.editNoteModal);

    // Refresh stream
    state.currentStreamItems = await dbManager.getStreamItems(state.activeTopicId);
    state.currentStreamItems.sort((a, b) => a.createdAt - b.createdAt);
    renderStreamFlow();
});

// Delete Item
async function deleteStreamItem(itemId) {
    if (confirm('Ar tikrai norite ištrinti šį įrašą iš srauto?')) {
        await dbManager.deleteStreamItem(itemId);
        
        // Refresh stream
        state.currentStreamItems = await dbManager.getStreamItems(state.activeTopicId);
        state.currentStreamItems.sort((a, b) => a.createdAt - b.createdAt);
        renderStreamFlow();
        renderTopicsList(); // update badge counts
    }
}

// Large Image Viewer
function openImageViewer(src, title, desc) {
    dom.viewerImg.src = src;
    dom.viewerCaption.innerHTML = `<strong>${escapeHtml(title)}</strong> ${desc ? `- ${escapeHtml(desc)}` : ''}`;
    openModal(dom.imageViewerModal);
}

// ==========================================================================
// IMPORT & EXPORT LOGIC
// ==========================================================================

// Export Data (JSON)
document.addEventListener('click', async (e) => {
    const btn = e.target.closest('#export-btn, .export-btn-action');
    if (!btn) return;
    try {
        const data = await dbManager.getAllData();
        const jsonString = `data:text/json;charset=utf-8,${encodeURIComponent(JSON.stringify(data, null, 2))}`;
        const downloadAnchor = document.createElement('a');
        
        const dateStr = new Date().toISOString().slice(0, 10);
        downloadAnchor.setAttribute('href', jsonString);
        downloadAnchor.setAttribute('download', `ai-uzrasai-srautas-${dateStr}.json`);
        document.body.appendChild(downloadAnchor);
        downloadAnchor.click();
        downloadAnchor.remove();
    } catch (e) {
        console.error('Nepavyko eksportuoti duomenų:', e);
        alert('Klaida eksportuojant duomenis.');
    }
});

// Import Data (JSON) trigger
document.addEventListener('click', (e) => {
    const btn = e.target.closest('#import-btn, .import-btn-action');
    if (!btn) return;
    dom.importFileInput.click();
});

dom.importFileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async (event) => {
        try {
            const data = JSON.parse(event.target.result);
            if (!data.topics) {
                throw new Error('Neteisingas failo formatas.');
            }
            if (confirm('Importuojant duomenis bus ištrintos visos dabartinės temos ir įrašai. Ar tikrai norite tęsti?')) {
                await dbManager.importAllData(data);
                alert('Duomenys sėkmingai importuoti!');
                state.activeTopicId = null;
                await loadAppState();
            }
        } catch (err) {
            console.error('Klaida importuojant duomenis:', err);
            alert('Nepavyko nuskaityti atsarginės kopijos failo. Patikrinkite formatą.');
        }
    };
    reader.readAsText(file);
    dom.importFileInput.value = ''; // Reset input
});

// Load Demo Template
dom.quickDemoBtn.addEventListener('click', async () => {
    if (confirm('Užkraunant pavyzdinį kursą bus sukurta demonstracinių duomenų jūsų sraute. Ar tęsti?')) {
        try {
            await dbManager.importAllData(demoData);
            state.activeTopicId = 'demo-lecture-1';
            await loadAppState();
        } catch (e) {
            console.error('Nepavyko užkrauti demo pavyzdžių:', e);
            alert('Klaida užkraunant demonstracinius duomenis.');
        }
    }
});

// ==========================================================================
// SEARCH & FILTER LOGIC
// ==========================================================================
let searchDebounceTimeout = null;

dom.searchInput.addEventListener('input', (e) => {
    const value = e.target.value;
    state.searchQuery = value;
    
    dom.clearSearchBtn.classList.toggle('hidden', value === '');

    clearTimeout(searchDebounceTimeout);
    searchDebounceTimeout = setTimeout(() => {
        renderTopicsList();
        if (state.activeTopicId) {
            renderStreamFlow();
        }
    }, 150);
});

dom.clearSearchBtn.addEventListener('click', () => {
    dom.searchInput.value = '';
    state.searchQuery = '';
    dom.clearSearchBtn.classList.add('hidden');
    renderTopicsList();
    if (state.activeTopicId) {
        renderStreamFlow();
    }
});

// ==========================================================================
// TEXT FORMATTING TOOLBAR & EMOJI PICKER ACTIONS
// ==========================================================================

function insertTextAtCursor(textarea, beforeVal, afterVal) {
    const scrollTop = textarea.scrollTop;
    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const text = textarea.value;
    const selectedText = text.substring(start, end);
    const replacement = beforeVal + selectedText + afterVal;
    
    textarea.value = text.substring(0, start) + replacement + text.substring(end);
    textarea.focus();
    
    // Restore scroll position so cursor doesn't jump to bottom
    textarea.scrollTop = scrollTop;
    
    // Position cursor right after the inserted before-text (inside wrapping tags)
    const newCursorPos = start + beforeVal.length + selectedText.length;
    textarea.selectionStart = newCursorPos;
    textarea.selectionEnd = newCursorPos;
}

const LARGE_EMOJI_LIST = [
    // Smegenys / Mokymasis / Technologijos
    '🧠','💡','🚀','💻','🤖','🎓','📝','🔍','⚡','🛠️','🎨','📊','🌐','🔑','🎯','📢',
    '📁','⭐','🔥','📚','📖','✏️','📐','🔬','🧪','🧬','🖥️','💾','💿','📂','💼','📅',
    '📌','📎','🔒','🔓','⚙️','🔧','🔨','⛏️','🪛','🪜','🧲','💈','🔭','🔮','🧿','🪬',
    // Veidai / Emocijos
    '😀','😃','😄','😁','😆','😅','🤣','😂','🙂','🙃','😉','😊','😇','🥰','😍','🤩',
    '😘','😗','😚','😙','😋','😛','😜','🤪','😝','🤑','🤗','🤭','🫢','🫣','🤫','🤔',
    '🤐','🤨','😐','😑','😶','😏','😒','🙄','😬','🤥','😌','😔','😪','🤤','😴','😷',
    '🤒','🤕','🤢','🤧','🥵','🥶','🥴','😵','🤯','🤠','🥳','🥸','😎','🤓','🧐','😕',
    '😟','🙁','☹️','😮','😯','😲','😳','🥺','😦','😧','😨','😰','😥','😢','😭','😱',
    '😖','😣','😞','😓','😩','😫','🥱','😤','😡','😠','🤬','😈','👿','💀','☠️','💩',
    '🤡','👹','👺','👻','👽','👾','🤖',
    // Gestai / Žmonės
    '👍','👎','👏','🙌','🤲','🤝','🙏','✌️','🤞','🤟','🤘','🤙','👌','🤌','🤏',
    '👈','👉','👆','👇','☝️','✋','🤚','🖐️','🖖','👋','🤙','💪','🦾','🦿','🦵','🦶',
    '👀','👁️','👂','🦻','👃','🫀','🫁','🧠','🦷','🦴','👅','👄','🫦',
    '🙋','🙋‍♂️','🙋‍♀️','🤦','🤷','💁','🙅','🙆','🧏',
    // Gyvūnai / Gamta
    '🐶','🐱','🐭','🐹','🐰','🦊','🐻','🐼','🐨','🐯','🦁','🐮','🐷','🐸','🐵',
    '🐔','🐧','🐦','🦆','🦅','🦉','🦇','🐝','🪱','🐛','🦋','🐌','🐞','🐜','🦟',
    '🌸','🌺','🌻','🌹','🌷','🌱','🌿','🍀','🍃','🍂','🍁','🌾','🌵','🌴','🌲','🌳',
    '⛅','🌤️','🌈','⛈️','🌊','🔥','💧','🌙','⭐','🌟','💫','✨','☄️','🌍','🌏','🌎',
    // Maistas / Gėrimai
    '🍎','🍊','🍋','🍇','🍓','🫐','🍒','🍑','🥭','🍍','🥥','🥝','🍅','🥕','🥦',
    '🍕','🍔','🍟','🌭','🥪','🌮','🌯','🥗','🍜','🍝','🍛','🍣','🍱','🥡','🍩',
    '🎂','🍰','🧁','🍫','🍬','🍭','☕','🍵','🧃','🥤','🧋','🍺','🍻','🥂','🍷',
    // Kelionės / Vietos
    '🚗','🚕','🚙','🚌','🚎','🏎️','🚓','🚑','🚒','✈️','🚀','🛸','🚂','🚢','🛥️',
    '🏠','🏡','🏢','🏣','🏤','🏥','🏦','🏨','🏩','🏪','🏫','🏬','🏭','🏯','🏰',
    '🗼','🗽','⛩️','🗿','🗺️','🧭','🏔️','⛰️','🌋','🏕️','🏖️','🏜️','🏝️','🌅','🌄',
    // Daiktai / Simboliai
    '💎','🏆','🥇','🎖️','🏅','🎗️','🎟️','🎫','🎪','🎭','🎨','🖼️','🎬','🎤','🎧',
    '🎵','🎶','🎸','🎹','🥁','🎺','🎻','🪕','🎮','🕹️','🎲','🧩','🪀','🪁','🎯',
    '📱','📲','💻','🖥️','🖨️','⌨️','🖱️','📡','📺','📻','📷','📸','📹','🎥','📞',
    '📟','📠','🔋','🪫','🔌','💡','🔦','🕯️','🪔','💰','💵','💳','💸','🏧','🛒',
    '📦','📫','📬','📭','📮','🗃️','🗄️','🗑️','📊','📈','📉','📋','📌','📍','✂️',
    '🔗','📏','📐','🔑','🗝️','🔐','🔒','🔓','🔨','⚒️','🛡️','⚔️','🪤','💊','🩺',
    // Ženklai
    '✅','❌','❓','❗','💯','🔴','🟠','🟡','🟢','🔵','🟣','⚫','⚪','🔶','🔷',
    '🔸','🔹','▶️','⏩','⏪','⏫','⏬','⏏️','🔀','🔁','🔂','⏱️','⏰','🕰️','⌛',
    '💬','💭','🗯️','💢','💥','💦','💨','🕳️','💤','💣','🔔','🔕','🎵','🎶','✨',
    '🎉','🎊','🎈','🎀','🎁','🎗️','🏳️','🏴','🚩','🏁','🎌','👑','💍','💎','🔮',
];

function initGenericEmojiPicker(btnId, dropdownId, textareaId) {
    const btn = document.getElementById(btnId);
    const dropdown = document.getElementById(dropdownId);
    const textarea = document.getElementById(textareaId);
    
    if (!btn || !dropdown || !textarea) return;
    
    dropdown.innerHTML = '';
    LARGE_EMOJI_LIST.forEach(emoji => {
        const item = document.createElement('span');
        item.className = 'emoji-item';
        item.textContent = emoji;
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            insertTextAtCursor(textarea, emoji, '');
            dropdown.classList.add('hidden');
        });
        dropdown.appendChild(item);
    });

    btn.addEventListener('click', (e) => {
        e.stopPropagation();
        dropdown.classList.toggle('hidden');
    });

    // Close dropdown on clicking outside
    document.addEventListener('click', (e) => {
        if (dropdown && !dropdown.contains(e.target) && !btn.contains(e.target)) {
            dropdown.classList.add('hidden');
        }
    });
}

function initGenericFormattingToolbar(toolbarId, textareaId) {
    const buttons = document.querySelectorAll(`#${toolbarId} .toolbar-btn[data-format]`);
    const textarea = document.getElementById(textareaId);
    if (!textarea) return;
    buttons.forEach(btn => {
        btn.addEventListener('click', () => {
            applyFormattingToTextarea(textarea, btn.dataset.format);
        });
    });
}

function initTopicEmojiPicker() {
    // Use the same full emoji list for topic icon picker
    const topicEmojis = LARGE_EMOJI_LIST;
    
    dom.topicEmojiDropdown.innerHTML = '';
    topicEmojis.forEach(emoji => {
        const item = document.createElement('span');
        item.className = 'emoji-item';
        item.textContent = emoji;
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            dom.topicEmojiInput.value = emoji;
            dom.topicEmojiDropdown.classList.add('hidden');
        });
        dom.topicEmojiDropdown.appendChild(item);
    });

    const toggleDropdown = (e) => {
        e.stopPropagation();
        dom.topicEmojiDropdown.classList.toggle('hidden');
    };

    dom.topicEmojiPickerBtn.addEventListener('click', toggleDropdown);
    dom.topicEmojiInput.addEventListener('click', toggleDropdown);

    // Close dropdown on clicking outside
    document.addEventListener('click', (e) => {
        if (dom.topicEmojiDropdown && !dom.topicEmojiDropdown.contains(e.target) && !dom.topicEmojiPickerBtn.contains(e.target) && e.target !== dom.topicEmojiInput) {
            dom.topicEmojiDropdown.classList.add('hidden');
        }
    });
}

function applyFormattingToTextarea(textarea, format) {
    const scrollTop = textarea.scrollTop;
    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const text = textarea.value;
    const selectedText = text.substring(start, end);

    if (format === 'bold' || format === 'italic') {
        const tag = format === 'bold' ? '**' : '*';
        
        if (selectedText.includes('\n')) {
            // Smart multi-line formatting (handles lists and lines independently)
            const lines = selectedText.split('\n');
            const formattedLines = lines.map(line => {
                // Matches list markers like "- ", "* ", "1. ", or "• "
                const listMatch = line.match(/^(\s*(?:-\s+|\*\s+|\d+\.\s+|•\s+))(.*)$/);
                if (listMatch) {
                    const marker = listMatch[1];
                    const content = listMatch[2];
                    if (content.trim()) {
                        return `${marker}${tag}${content}${tag}`;
                    }
                    return line;
                } else {
                    if (line.trim()) {
                        return `${tag}${line}${tag}`;
                    }
                    return line;
                }
            });
            
            const replacement = formattedLines.join('\n');
            textarea.value = text.substring(0, start) + replacement + text.substring(end);
            textarea.focus();
            textarea.scrollTop = scrollTop;
            textarea.selectionStart = start;
            textarea.selectionEnd = start + replacement.length;
        } else {
            // Check if single line has a list marker
            const listMatch = selectedText.match(/^(\s*(?:-\s+|\*\s+|\d+\.\s+|•\s+))(.*)$/);
            if (listMatch) {
                const marker = listMatch[1];
                const content = listMatch[2];
                const replacement = `${marker}${tag}${content}${tag}`;
                textarea.value = text.substring(0, start) + replacement + text.substring(end);
                textarea.focus();
                textarea.scrollTop = scrollTop;
                textarea.selectionStart = start + marker.length;
                textarea.selectionEnd = start + marker.length + tag.length + content.length + tag.length;
            } else {
                insertTextAtCursor(textarea, tag, tag);
            }
        }
    } else if (format === 'bullet') {
        const isNewLine = start === 0 || text.charAt(start - 1) === '\n';
        insertTextAtCursor(textarea, isNewLine ? '- ' : '\n- ', '');
    } else if (format === 'number') {
        const isNewLine = start === 0 || text.charAt(start - 1) === '\n';
        insertTextAtCursor(textarea, isNewLine ? '1. ' : '\n1. ', '');
    } else if (format === 'code') {
        if (selectedText.includes('\n') || selectedText.length > 30) {
            insertTextAtCursor(textarea, '\n```\n', '\n```\n');
        } else {
            insertTextAtCursor(textarea, '`', '`');
        }
    }
}

function initTextStylingPickers(prefix = '', textareaId = '') {
    let textarea;
    if (textareaId) {
        textarea = document.getElementById(textareaId);
    } else {
        const isEdit = prefix === 'edit-note-';
        textarea = isEdit ? dom.editNoteContent : dom.captureInput;
    }
    if (!textarea) return;
    
    const colorBtn = document.getElementById(prefix + 'color-picker-btn');
    const colorDropdown = document.getElementById(prefix + 'color-dropdown');
    const highlightBtn = document.getElementById(prefix + 'highlight-picker-btn');
    const highlightDropdown = document.getElementById(prefix + 'highlight-dropdown');
    const sizeBtn = document.getElementById(prefix + 'size-picker-btn');
    const sizeDropdown = document.getElementById(prefix + 'size-dropdown');
    
    if (!colorBtn || !colorDropdown || !highlightBtn || !highlightDropdown || !sizeBtn || !sizeDropdown) return;
    
    const closeAllDropdowns = () => {
        colorDropdown.classList.add('hidden');
        highlightDropdown.classList.add('hidden');
        sizeDropdown.classList.add('hidden');
        
        const emojiDropdown = document.getElementById(prefix + 'emoji-dropdown');
        if (emojiDropdown) emojiDropdown.classList.add('hidden');
    };
    
    colorBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        const wasHidden = colorDropdown.classList.contains('hidden');
        closeAllDropdowns();
        if (wasHidden) colorDropdown.classList.remove('hidden');
    });
    
    highlightBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        const wasHidden = highlightDropdown.classList.contains('hidden');
        closeAllDropdowns();
        if (wasHidden) highlightDropdown.classList.remove('hidden');
    });
    
    sizeBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        const wasHidden = sizeDropdown.classList.contains('hidden');
        closeAllDropdowns();
        if (wasHidden) sizeDropdown.classList.remove('hidden');
    });
    
    colorDropdown.querySelectorAll('.color-swatch').forEach(swatch => {
        swatch.addEventListener('click', (e) => {
            e.stopPropagation();
            const color = swatch.dataset.color;
            insertTextAtCursor(textarea, `<span style="color: ${color};">`, '</span>');
            colorDropdown.classList.add('hidden');
        });
    });
    
    highlightDropdown.querySelectorAll('.highlight-swatch').forEach(swatch => {
        swatch.addEventListener('click', (e) => {
            e.stopPropagation();
            const bg = swatch.dataset.bg;
            const color = swatch.dataset.color;
            insertTextAtCursor(textarea, `<mark style="background: ${bg}; color: ${color}; padding: 2px 4px; border-radius: 4px;">`, '</mark>');
            highlightDropdown.classList.add('hidden');
        });
    });
    
    sizeDropdown.querySelectorAll('.size-option').forEach(option => {
        option.addEventListener('click', (e) => {
            e.stopPropagation();
            const size = option.dataset.size;
            insertTextAtCursor(textarea, `<span style="font-size: ${size}; display: inline-block;">`, '</span>');
            sizeDropdown.classList.add('hidden');
        });
    });
    
    document.addEventListener('click', (e) => {
        if (!colorDropdown.contains(e.target) && !colorBtn.contains(e.target) &&
            !highlightDropdown.contains(e.target) && !highlightBtn.contains(e.target) &&
            !sizeDropdown.contains(e.target) && !sizeBtn.contains(e.target)) {
            closeAllDropdowns();
        }
    });
}

// ==========================================================================
// MODAL STATE HELPERS
// ==========================================================================

function openModal(modal) {
    modal.classList.add('show');
}

function closeModal(modal) {
    modal.classList.remove('show');
    if (modal === dom.topicModal) {
        dom.topicForm.reset();
        dom.topicModalId.value = '';
        dom.topicModalParentId.value = '';
    } else if (modal === dom.editNoteModal) {
        dom.editNoteForm.reset();
        dom.editNoteId.value = '';
        dom.editNoteType.value = '';
        dom.editNoteImageGroup.classList.add('hidden');
        dom.editNoteImagePreview.src = '';
    }
}

function registerEventListeners() {
    // Mobile Sidebar Toggles
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    
    document.addEventListener('click', (e) => {
        const toggleBtn = e.target.closest('.toggle-sidebar-btn');
        if (toggleBtn) {
            sidebar.classList.toggle('mobile-open');
            overlay.classList.toggle('active');
        } else if (e.target === overlay || e.target.closest('.sidebar-footer button, #topics-list li')) {
            sidebar.classList.remove('mobile-open');
            overlay.classList.remove('active');
        }
    });

    // Initialize toolbar and emoji pickers - Quick Capture
    initGenericEmojiPicker('emoji-picker-btn', 'emoji-dropdown', 'capture-input');
    initGenericFormattingToolbar('capture-toolbar', 'capture-input');
    initTextStylingPickers('');

    // Initialize toolbar and emoji pickers - Edit Note Modal (Turinys)
    initGenericEmojiPicker('edit-note-emoji-picker-btn', 'edit-note-emoji-dropdown', 'edit-note-content');
    initGenericFormattingToolbar('edit-note-toolbar', 'edit-note-content');
    initTextStylingPickers('edit-note-');

    // Initialize toolbar and emoji pickers - Edit Note Modal (Antraštė)
    initGenericEmojiPicker('edit-note-title-emoji-picker-btn', 'edit-note-title-emoji-dropdown', 'edit-note-title');
    initGenericFormattingToolbar('edit-note-title-toolbar', 'edit-note-title');
    initTextStylingPickers('edit-note-title-', 'edit-note-title');

    // Initialize toolbar and emoji pickers - Topic Modal (Pavadinimas)
    initGenericEmojiPicker('topic-title-emoji-picker-btn', 'topic-title-emoji-dropdown', 'topic-title');
    initGenericFormattingToolbar('topic-title-toolbar', 'topic-title');
    initTextStylingPickers('topic-title-', 'topic-title');

    // Initialize toolbar and emoji pickers - Topic Modal (Aprašymas)
    initGenericEmojiPicker('topic-desc-emoji-picker-btn', 'topic-desc-emoji-dropdown', 'topic-desc');
    initGenericFormattingToolbar('topic-desc-toolbar', 'topic-desc');
    initTextStylingPickers('topic-desc-', 'topic-desc');

    // Topic icon emoji picker (special - writes to input not textarea)
    initTopicEmojiPicker();

    // Welcome views
    dom.welcomeNewTopicBtn.addEventListener('click', () => {
        dom.topicModalId.value = '';
        dom.topicModalParentId.value = '';
        dom.topicTitleInput.placeholder = 'Pvz., Dirbtinio Intelekto Kursas';
        dom.topicEmojiInput.value = '🧠';
        dom.topicModalTitle.textContent = 'Sukurti naują temą (kursą)';
        openModal(dom.topicModal);
    });

    dom.newTopicBtn.addEventListener('click', () => {
        dom.topicModalId.value = '';
        dom.topicModalParentId.value = '';
        dom.topicTitleInput.placeholder = 'Pvz., Dirbtinio Intelekto Kursas';
        dom.topicEmojiInput.value = '🧠';
        dom.topicModalTitle.textContent = 'Sukurti naują temą (kursą)';
        openModal(dom.topicModal);
    });

    // Dashboard new subtopic click
    dom.dashboardNewSubtopicBtn.addEventListener('click', () => {
        if (!state.activeTopicId) return;
        const topic = state.topics.find(t => t.id === state.activeTopicId);
        if (topic && !topic.parentId) {
            openAddSubtopicModal(topic.id);
        }
    });

    // Topic edit/delete
    dom.editTopicBtn.addEventListener('click', () => {
        if (!state.activeTopicId) return;
        const topic = state.topics.find(t => t.id === state.activeTopicId);
        if (!topic) return;

        dom.topicModalId.value = topic.id;
        dom.topicTitleInput.value = topic.title;
        dom.topicEmojiInput.value = topic.emoji || (topic.parentId ? '💡' : '🧠');
        dom.topicDescInput.value = topic.description || '';
        
        if (topic.parentId) {
            dom.topicTitleInput.placeholder = 'Pvz., 1 Paskaita: Promptai';
            dom.topicModalTitle.textContent = 'Redaguoti potemę (paskaitą)';
        } else {
            dom.topicTitleInput.placeholder = 'Pvz., Dirbtinio Intelekto Kursas';
            dom.topicModalTitle.textContent = 'Redaguoti temą (kursą)';
        }
        openModal(dom.topicModal);
    });

    dom.deleteTopicBtn.addEventListener('click', deleteTopic);

    // Save Capture btn click and Enter keys
    dom.saveCaptureBtn.addEventListener('click', saveQuickCapture);
    
    // Support Ctrl+Enter to save capture
    dom.captureInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && e.ctrlKey) {
            e.preventDefault();
            saveQuickCapture();
        }
    });

    // Close Modals
    document.querySelectorAll('.close-modal-btn, .cancel-modal-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            const modal = btn.closest('.modal');
            closeModal(modal);
        });
    });

    // Close viewer modal on click or X
    dom.closeViewerBtn.addEventListener('click', () => closeModal(dom.imageViewerModal));
    dom.imageViewerModal.addEventListener('click', (e) => {
        if (e.target === dom.imageViewerModal) {
            closeModal(dom.imageViewerModal);
        }
    });

    // Escape key closes modals
    window.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal.show').forEach(modal => {
                closeModal(modal);
            });
        }
    });

    // Initialize Drag & Drop
    initDragAndDrop();
}



// ==========================================================================================================
// DRAG AND DROP FILES LOGIC
// ==========================================================================
function initDragAndDrop() {
    const box = dom.quickCaptureBox;
    if (!box) return;

    ['dragenter', 'dragover'].forEach(eventName => {
        box.addEventListener(eventName, (e) => {
            e.preventDefault();
            box.style.borderColor = 'var(--secondary)';
            box.style.background = 'rgba(6, 182, 212, 0.05)';
        }, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        box.addEventListener(eventName, (e) => {
            e.preventDefault();
            box.style.borderColor = '';
            box.style.background = '';
        }, false);
    });

    box.addEventListener('drop', (e) => {
        const dt = e.dataTransfer;
        const files = dt.files;
        if (files) {
            for (let i = 0; i < files.length; i++) {
                handleSelectedFile(files[i]);
            }
        }
    });
}

// ==========================================================================
// ATTACHMENT OCR LOGIC
// ==========================================================================
async function runOcrForAttachment(id) {
    const att = state.currentAttachments.find(a => a.id === id);
    if (!att) return;
    
    // Find the OCR button by data-id
    const ocrBtn = document.querySelector(`.btn-preview-ocr[data-id="${id}"]`);
    if (!ocrBtn) return;
    
    const textarea = dom.captureInput;
    
    ocrBtn.disabled = true;
    const originalText = ocrBtn.innerHTML;
    ocrBtn.innerHTML = `<i data-lucide="loader-2" class="spin" style="width:12px; height:12px; margin-right:4px;"></i> Nuskaitoma...`;
    lucide.createIcons();
    
    try {
        const result = await Tesseract.recognize(
            att.data,
            'lit+eng',
            {
                logger: m => {
                    if (m.status === 'recognizing text') {
                        ocrBtn.innerHTML = `<i data-lucide="loader-2" class="spin" style="width:12px; height:12px; margin-right:4px;"></i> ${Math.round(m.progress * 100)}%...`;
                        lucide.createIcons();
                    }
                }
            }
        );
        
        const text = result.data.text;
        if (text && text.trim().length > 0) {
            const scrollTop = textarea.scrollTop;
            const currentVal = textarea.value.trim();
            textarea.value = (currentVal ? currentVal + '\n\n' : '') + text.trim();
            textarea.focus();
            textarea.scrollTop = scrollTop;
            // Auto-remove the image attachment after extracting text
            removeAttachment(id);
        } else {
            alert('Nepavyko atpažinti jokio teksto šioje nuotraukoje.');
        }
    } catch (e) {
        console.error('OCR error:', e);
        alert('Nepavyko nuskaityti nuotraukos. Patikrinkite interneto ryšį.');
    } finally {
        ocrBtn.disabled = false;
        ocrBtn.innerHTML = originalText;
        lucide.createIcons();
    }
}

// Extract text from a SAVED file-type stream card and convert it to text type
async function extractAndHidePdfOnCard(item, btn) {
    if (!item.fileData) return;
    
    const originalHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = `<i data-lucide="loader-2" class="spin" style="width:12px;height:12px;margin-right:4px;"></i> Nuskaitoma...`;
    lucide.createIcons();

    try {
        const loadingTask = pdfjsLib.getDocument(item.fileData);
        const pdf = await loadingTask.promise;
        let fullText = '';

        for (let pageNum = 1; pageNum <= pdf.numPages; pageNum++) {
            btn.innerHTML = `<i data-lucide="loader-2" class="spin" style="width:12px;height:12px;margin-right:4px;"></i> Psl. ${pageNum}/${pdf.numPages}...`;
            lucide.createIcons();
            const page = await pdf.getPage(pageNum);
            const textContent = await page.getTextContent();
            const pageText = textContent.items.map(i => i.str).join(' ');
            fullText += pageText + '\n\n';
        }

        if (fullText.trim().length > 0) {
            // Update the item: convert to text type, save extracted text, remove file data
            const existing = await dbManager.getStreamItem(item.id);
            if (existing) {
                existing.type = 'text';
                existing.content = (existing.content ? existing.content + '\n\n' : '') + fullText.trim();
                existing.fileData = null;
                existing.fileType = null;
                existing.fileSize = null;
                existing.updatedAt = Date.now();
                await dbManager.updateStreamItem(existing);

                // Refresh stream
                state.currentStreamItems = await dbManager.getStreamItems(state.activeTopicId);
                state.currentStreamItems.sort((a, b) => a.createdAt - b.createdAt);
                renderStreamFlow();
            }
        } else {
            alert('Nepavyko atpažinti jokio tekstinio turinio šiame PDF.');
            btn.disabled = false;
            btn.innerHTML = originalHtml;
            lucide.createIcons();
        }
    } catch (e) {
        console.error('PDF extraction error:', e);
        alert('Įvyko klaida skaitant PDF dokumentą.');
        btn.disabled = false;
        btn.innerHTML = originalHtml;
        lucide.createIcons();
    }
}

async function runPdfOcrForAttachment(id) {
    const att = state.currentAttachments.find(a => a.id === id);
    if (!att) return;
    
    const ocrBtn = document.querySelector(`.btn-preview-ocr[data-id="${id}"]`);
    if (!ocrBtn) return;
    
    const textarea = dom.captureInput;
    
    ocrBtn.disabled = true;
    const originalText = ocrBtn.innerHTML;
    ocrBtn.innerHTML = `<i data-lucide="loader-2" class="spin" style="width:12px; height:12px; margin-right:4px;"></i> Nuskaitoma...`;
    lucide.createIcons();
    
    try {
        const loadingTask = pdfjsLib.getDocument(att.data);
        const pdf = await loadingTask.promise;
        let fullText = '';
        
        for (let pageNum = 1; pageNum <= pdf.numPages; pageNum++) {
            ocrBtn.innerHTML = `<i data-lucide="loader-2" class="spin" style="width:12px; height:12px; margin-right:4px;"></i> Psl. ${pageNum}/${pdf.numPages}...`;
            lucide.createIcons();
            
            const page = await pdf.getPage(pageNum);
            const textContent = await page.getTextContent();
            const pageText = textContent.items.map(item => item.str).join(' ');
            fullText += pageText + '\n\n';
        }
        
        if (fullText.trim().length > 0) {
            const scrollTop = textarea.scrollTop;
            const currentVal = textarea.value.trim();
            textarea.value = (currentVal ? currentVal + '\n\n' : '') + fullText.trim();
            textarea.focus();
            textarea.scrollTop = scrollTop;
            // Auto-remove the PDF attachment after extracting text - no need to show the file anymore
            removeAttachment(id);
        } else {
            alert('Nepavyko atpažinti jokio tekstinio turinio šiame PDF.');
        }
    } catch (e) {
        console.error('PDF extraction error:', e);
        alert('Įvyko klaida skaitant PDF dokumentą.');
    } finally {
        ocrBtn.disabled = false;
        ocrBtn.innerHTML = originalText;
        lucide.createIcons();
    }
}

// ==========================================================================
// STRING ESCAPING HELPERS (Prevent XSS)
// ==========================================================================
function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function escapeAttribute(unsafe) {
    if (!unsafe) return '';
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
