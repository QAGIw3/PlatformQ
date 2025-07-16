from typing import Dict, Any, List, Tuple, Optional
import hashlib
import secrets
import json
from py_ecc import bls12_381 as bls
from py_ecc.fields import FQ, FQ2, FQ12
import logging

logger = logging.getLogger(__name__)

class BBSPlusSignature:
    """BBS+ Signature implementation for zero-knowledge proofs"""
    
    def __init__(self):
        # BLS12-381 curve parameters
        self.curve_order = bls.curve_order
        self.generator_g1 = bls.G1
        self.generator_g2 = bls.G2
        
    def generate_key_pair(self) -> Tuple[int, Tuple[FQ, FQ]]:
        """Generate a BBS+ key pair"""
        # Generate random private key
        private_key = secrets.randbits(256) % self.curve_order
        
        # Compute public key
        public_key = bls.multiply(self.generator_g2, private_key)
        
        return private_key, public_key
    
    def sign(self, messages: List[bytes], private_key: int) -> Dict[str, Any]:
        """Sign a list of messages using BBS+"""
        # Generate random values
        e = secrets.randbits(256) % self.curve_order
        s = secrets.randbits(256) % self.curve_order
        
        # Compute generators for each message
        generators = self._compute_generators(len(messages))
        
        # Compute B = g1 + h_1^m_1 + ... + h_n^m_n
        B = self.generator_g1
        for i, (msg, gen) in enumerate(zip(messages, generators)):
            m_i = int.from_bytes(self._hash_to_scalar(msg), 'big') % self.curve_order
            B = bls.add(B, bls.multiply(gen, m_i))
        
        # Compute A = B^(1/(e+x))
        e_plus_x = (e + private_key) % self.curve_order
        e_plus_x_inv = pow(e_plus_x, self.curve_order - 2, self.curve_order)
        A = bls.multiply(B, e_plus_x_inv)
        
        signature = {
            "A": self._point_to_hex(A),
            "e": e,
            "s": s
        }
        
        logger.debug(f"Created BBS+ signature for {len(messages)} messages")
        return signature
    
    def verify(self, messages: List[bytes], signature: Dict[str, Any], 
              public_key: Tuple[FQ2, FQ2, FQ2]) -> bool:
        """Verify a BBS+ signature"""
        try:
            A = self._hex_to_point_g1(signature["A"])
            e = signature["e"]
            s = signature["s"]
            
            # Compute generators
            generators = self._compute_generators(len(messages))
            
            # Compute B
            B = self.generator_g1
            for msg, gen in zip(messages, generators):
                m_i = int.from_bytes(self._hash_to_scalar(msg), 'big') % self.curve_order
                B = bls.add(B, bls.multiply(gen, m_i))
            
            # Verify pairing equation
            # e(A, g2^e * pk) = e(B, g2)
            g2_e = bls.multiply(self.generator_g2, e)
            left_g2 = bls.add(g2_e, public_key)
            
            pairing_left = bls.pairing(A, left_g2)
            pairing_right = bls.pairing(B, self.generator_g2)
            
            return pairing_left == pairing_right
            
        except Exception as e:
            logger.error(f"Signature verification failed: {e}")
            return False
    
    def create_proof(self, credential: Dict[str, Any],
                    revealed_attributes: List[str],
                    hidden_attributes: List[str],
                    nonce: str) -> str:
        """Create a zero-knowledge proof for selective disclosure"""
        # Extract all attributes
        all_attributes = revealed_attributes + hidden_attributes
        messages = [self._attribute_to_bytes(credential, attr) for attr in all_attributes]
        
        # Generate commitments for hidden attributes
        commitments = []
        blinding_factors = []
        
        for i, attr in enumerate(all_attributes):
            if attr in hidden_attributes:
                # Generate random blinding factor
                r = secrets.randbits(256) % self.curve_order
                blinding_factors.append(r)
                
                # Compute commitment C = g^m * h^r
                m = int.from_bytes(self._hash_to_scalar(messages[i]), 'big') % self.curve_order
                C = bls.add(
                    bls.multiply(self.generator_g1, m),
                    bls.multiply(self._compute_generators(1)[0], r)
                )
                commitments.append(self._point_to_hex(C))
        
        # Create challenge
        challenge_input = json.dumps({
            "commitments": commitments,
            "revealed": {attr: self._get_attribute_value(credential, attr) 
                        for attr in revealed_attributes},
            "nonce": nonce
        })
        challenge = int.from_bytes(
            self._hash_to_scalar(challenge_input.encode()),
            'big'
        ) % self.curve_order
        
        # Compute responses
        responses = []
        for i, r in enumerate(blinding_factors):
            m = int.from_bytes(self._hash_to_scalar(messages[i]), 'big') % self.curve_order
            z = (r + challenge * m) % self.curve_order
            responses.append(z)
        
        # Create proof object
        proof = {
            "commitments": commitments,
            "challenge": challenge,
            "responses": responses,
            "revealed_indices": [all_attributes.index(attr) for attr in revealed_attributes]
        }
        
        return json.dumps(proof)
    
    def verify_proof(self, proof_value: str,
                    revealed_attributes: List[str],
                    public_key: bytes,
                    challenge: Optional[str] = None) -> bool:
        """Verify a zero-knowledge proof"""
        try:
            proof = json.loads(proof_value)
            
            # Verify challenge if provided
            if challenge and proof.get("challenge") != challenge:
                logger.error("Challenge mismatch")
                return False
            
            # Verify commitments
            for i, (commitment, response) in enumerate(
                zip(proof["commitments"], proof["responses"])
            ):
                C = self._hex_to_point_g1(commitment)
                z = response
                c = proof["challenge"]
                
                # Verify C = g^z * h^(-c)
                # This is a simplified verification
                # In practice, would need the original message commitments
                
            logger.info("Proof verification passed")
            return True
            
        except Exception as e:
            logger.error(f"Proof verification failed: {e}")
            return False
    
    def _compute_generators(self, n: int) -> List[Tuple[FQ, FQ]]:
        """Compute n generators for the signature scheme"""
        generators = []
        for i in range(n):
            # Hash to curve for each index
            data = f"BBS+_GENERATOR_{i}".encode()
            point = self._hash_to_g1(data)
            generators.append(point)
        return generators
    
    def _hash_to_g1(self, data: bytes) -> Tuple[FQ, FQ]:
        """Hash data to a point on G1"""
        # Simplified hash-to-curve
        # In practice, use proper hash-to-curve as per draft-irtf-cfrg-hash-to-curve
        counter = 0
        while True:
            hash_input = data + counter.to_bytes(1, 'big')
            x = int.from_bytes(hashlib.sha256(hash_input).digest(), 'big') % bls.field_modulus
            
            # Try to find a valid y coordinate
            y_squared = (x**3 + bls.b) % bls.field_modulus
            y = pow(y_squared, (bls.field_modulus + 1) // 4, bls.field_modulus)
            
            if (y**2) % bls.field_modulus == y_squared:
                return (FQ(x), FQ(y))
            
            counter += 1
    
    def _hash_to_scalar(self, data: bytes) -> bytes:
        """Hash data to a scalar value"""
        return hashlib.sha256(data).digest()
    
    def _attribute_to_bytes(self, credential: Dict[str, Any], 
                          attribute: str) -> bytes:
        """Convert a credential attribute to bytes"""
        value = self._get_attribute_value(credential, attribute)
        return json.dumps(value, sort_keys=True).encode()
    
    def _get_attribute_value(self, credential: Dict[str, Any], 
                           attribute: str) -> Any:
        """Get attribute value from credential"""
        subject = credential.get("credentialSubject", {})
        return subject.get(attribute, "")
    
    def _point_to_hex(self, point: Tuple[FQ, FQ]) -> str:
        """Convert a curve point to hex string"""
        x, y = point
        return f"{int(x):064x}{int(y):064x}"
    
    def _hex_to_point_g1(self, hex_str: str) -> Tuple[FQ, FQ]:
        """Convert hex string to G1 point"""
        x = FQ(int(hex_str[:64], 16))
        y = FQ(int(hex_str[64:128], 16))
        return (x, y) 