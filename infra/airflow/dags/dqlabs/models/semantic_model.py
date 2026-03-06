import re
from transformers import pipeline
from typing import Any, Dict, Optional, List, Tuple, Union
from dqlabs.app_helper.log_helper import log_info


class ZeroShotClassifier:
    def __init__(self):
        self.classifier = pipeline(
            "zero-shot-classification", model="facebook/bart-large-mnli"
        )

    def _matches_email_pattern(self, data: List[str]) -> bool:
        """
        Check if any of the strings in the list match the email regex pattern.
        """
        email_pattern = r"[^@]+@[^@]+\.[^@]+"
        return any(re.match(email_pattern, item) for item in data)

    def _matches_ip_address_pattern(self, data: List[str]) -> bool:
        """
        Check if any of the strings in the list match the IP address regex pattern.
        """
        ip_pattern = r"^((?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\.){3}(?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$"
        return any(re.match(ip_pattern, item) for item in data)
    
    def _matches_address_pattern(self, data: List[str]) -> bool:
        """
        Check if any of the strings in the list match the specific address pattern.
        Pattern matches addresses like "123 Main St", "456 Elm St", etc.
        """
        # Street address pattern: number + street name + St
        # Also handles potential row numbers at the beginning
        street_pattern = r'(\d+\s+)?\d+\s+\w+\s+St$'
        
        for item in data:
            if not isinstance(item, str):
                continue
                
            # Clean up the item - remove leading/trailing whitespace
            item = item.strip()
            
            # Check if it matches the pattern
            if re.match(street_pattern, item, re.IGNORECASE):
                return True
                
        return False

    def classify_column_type(
        self, data: List[str], column_name: str = "", threshold: float = 0.4
    ) -> Optional[str]:
        """
        Classify the type of data in a column using regex and zero-shot classification.
        Now includes column name in the classification for better accuracy.
        
        Args:
            data: List of sample data values from the column
            column_name: Name of the column being classified
            threshold: Minimum confidence threshold for classification
            
        Returns:
            Classified semantic term or None if below threshold
        """
        labels = [
            "AddressLine1",
            "State",
            "FirstName",
            "MaritalStatus",
            "PhoneNumber",
            "SSN",
            "City",
            "Country",
            "Latitude",
            "Longitude",
        ]
        
        # Check for email pattern first
        if self._matches_email_pattern(data):
            log_info(f"Column '{column_name}' classified as Email via regex pattern")
            return "Email"

        # Check for IP address pattern
        if self._matches_ip_address_pattern(data):
            log_info(f"Column '{column_name}' classified as IPAddress via regex pattern")
            return "IPAddress"
            
        # Check for address pattern
        if self._matches_address_pattern(data):
            log_info(f"Column '{column_name}' classified as AddressLine1 via regex pattern")
            return "AddressLine1"

        # Prepare context for zero-shot classification
        # Include BOTH column name AND sample data values
        context_parts = []
        
        if column_name and column_name.strip():
            context_parts.append(f"Column name: {column_name.strip()}")
        
        # Add sample data values (limit to avoid token limits)
        if data:
            # Filter out None/empty values and take first 10
            valid_data = [str(item).strip() for item in data if item is not None and str(item).strip()]
            sample_data = valid_data[:10]
            
            if sample_data:
                joined_data = ", ".join(sample_data)
                context_parts.append(f"Sample values: {joined_data}")
        
        # Combine column name and sample data for full context
        full_context = ". ".join(context_parts)
        
        if not full_context:
            log_info("No context available for zero-shot classification")
            return None
        
        log_info(f"Zero-shot classification context: {full_context}")
        
        # Perform zero-shot classification with full context
        classification_result = self.classifier(full_context, labels)
        log_info(f"Classification result: {classification_result}")
        
        # Extract the label with the highest score
        scores = classification_result["scores"]
        result_labels = classification_result["labels"]

        # Get the index of the maximum score and check if it's above threshold
        max_score = max(scores)
        max_score_index = scores.index(max_score)

        classified_term = result_labels[max_score_index] if max_score > threshold else None
        
        if classified_term:
            # Special handling for country/state codes
            if classified_term.lower() in ("country", "state"):
                # If all values have length 2, classify as country/state code
                valid_data = [str(item).strip() for item in data if item is not None and str(item).strip()]
                if valid_data:
                    is_value_length_two_digits = all(len(value) == 2 for value in valid_data)
                    if is_value_length_two_digits:
                        classified_term = (
                            "Countrycode"
                            if classified_term.lower() == "country"
                            else "StateCode"
                        )
                        log_info(f"Adjusted classification to {classified_term} based on 2-character pattern")

        log_info(f"Final classified term for column '{column_name}': {classified_term} (confidence: {max_score:.3f})")
        return classified_term