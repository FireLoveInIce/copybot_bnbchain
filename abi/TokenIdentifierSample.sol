// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

interface ITokenManager {
    struct TokenInfo {
        address base;
        address quote;
        uint256 template;
        uint256 totalSupply;
        uint256 maxOffers;
        uint256 maxRaising;
        uint256 launchTime;
        uint256 offers;
        uint256 funds;
        uint256 lastPrice;
        uint256 K;
        uint256 T;
        uint256 status;
    }
    
    struct TokenInfoEx1 {
        uint256 launchFee;
        uint256 pcFee;
        uint256 feeSetting;
        uint256 blockNumber;
        uint256 extraFee;
    }
    
    function _tokenInfos(address token) external view returns (TokenInfo memory);
    function _tokenInfoEx1s(address token) external view returns (TokenInfoEx1 memory);
}

contract TokenIdentifierSample {
    address public constant TOKEN_MANAGER = 0x5c952063c7fc8610FFDB798152D69F0B9550762b;
    
    uint256 private constant CREATOR_OFFSET = 10;
    uint256 private constant CREATOR_BITS = 6;
    
    uint256 public constant TOKEN_TYPE_TAX = 5;  // TaxToken creator type
    
    /**
     * @notice Get the creator type of a token
     * @param tokenAddress Token address
     * @return creatorType Creator type value
     */
    function getCreatorType(address tokenAddress) public view returns (uint256) {
        ITokenManager.TokenInfo memory ti = ITokenManager(TOKEN_MANAGER)._tokenInfos(tokenAddress);
        return (ti.template >> CREATOR_OFFSET) & ((1 << CREATOR_BITS) - 1);
    }
    
    /**
     * @notice Check if a token is a TaxToken
     * @param tokenAddress Token address
     * @return isTaxToken True if the token is a TaxToken (creatorType == 5)
     */
    function isTaxToken(address tokenAddress) public view returns (bool) {
        return getCreatorType(tokenAddress) == TOKEN_TYPE_TAX;
    }
    
    /**
     * @notice Check if a token has AntiSniperFeeMode enabled
     * @param tokenAddress Token address
     * @return isAntiSniper True if AntiSniperFeeMode is enabled (feeSetting > 0)
     */
    function isAntiSniperFeeMode(address tokenAddress) public view returns (bool) {
        ITokenManager.TokenInfoEx1 memory tix1 = ITokenManager(TOKEN_MANAGER)._tokenInfoEx1s(tokenAddress);
        return tix1.feeSetting > 0;
    }
}
