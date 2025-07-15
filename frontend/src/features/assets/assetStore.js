import { create } from 'zustand';

/**
 * Zustand store for managing the state of the Asset Explorer.
 */
export const useAssetStore = create((set) => ({
    /**
     * The ID of the currently selected asset.
     * Null if no asset is selected.
     */
    selectedAssetId: null,

    /**
     * The current search term entered by the user.
     */
    searchTerm: '',

    /**
     * The asset type to filter by.
     */
    filterType: '',

    /**
     * Action to select an asset.
     * @param {string} assetId - The ID of the asset to select.
     */
    selectAsset: (assetId) => set({ selectedAssetId: assetId }),

    /**
     * Action to clear the current selection.
     */
    clearSelection: () => set({ selectedAssetId: null }),

    /**
     * Action to set the search term.
     * @param {string} term - The new search term.
     */
    setSearchTerm: (term) => set({ searchTerm: term }),

    /**
     * Action to set the filter type.
     * @param {string} type - The asset type to filter by.
     */
    setFilterType: (type) => set({ filterType: type }),
})); 