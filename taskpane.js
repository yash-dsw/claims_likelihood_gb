let mailboxItem = null;

Office.onReady((info) => {
    if (info.host === Office.HostType.Outlook) {
        // Store the mailbox item reference
        mailboxItem = Office.context.mailbox.item;
        
        // Display current email subject
        displayEmailInfo();
        
        console.log('Office.js initialized successfully');
    }
});

function displayEmailInfo() {
    if (!mailboxItem) {
        console.error('Mailbox item not available');
        return;
    }
    
    // For Outlook, subject is a property, not a method in read mode
    try {
        const subject = mailboxItem.subject;
        document.getElementById('emailSubject').textContent = subject || 'No subject';
        document.getElementById('emailInfo').style.display = 'block';
    } catch (error) {
        console.error('Error displaying email info:', error);
    }
}

async function triggerFlow() {
    const button = document.getElementById('triggerButton');
    const statusDiv = document.getElementById('statusMessage');
    
    // Check if Office context is available
    if (!mailboxItem) {
        showStatus('Outlook context not available. Please reload the add-in.', 'error');
        return;
    }
    
    // Disable button during request
    button.disabled = true;
    button.textContent = 'Triggering...';
    
    // Show info message
    showStatus('Sending request to Power Automate...', 'info');
    
    try {
        // Get email details to send to the flow
        const emailData = await getEmailData();
        
        // Replace with your Power Automate HTTP POST URL
        const flowUrl = 'https://default74afe875305e4ab4ba4ac1359a7629.ae.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/98694a6fe5ce4b1d8389f23d378bd9e0/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=jEGsyOkQ2_bKCUTqufTMr99gQw9x4Q5oPphpSI7fMEA';
        
        // Make the request to Power Automate
        const response = await fetch(flowUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(emailData)
        });
        
        if (response.ok) {
            const responseText = await response.text();
            console.log('Flow response:', responseText);
            showStatus('Flow triggered successfully!', 'success');
        } else {
            const errorText = await response.text();
            throw new Error(`HTTP error! status: ${response.status}, details: ${errorText}`);
        }
        
    } catch (error) {
        console.error('Error triggering flow:', error);
        showStatus('Failed to Underwriting: ' + error.message, 'error');
    } finally {
        // Re-enable button
        button.disabled = false;
        button.textContent = 'Underwriting';
    }
}

async function getEmailData() {
    return new Promise((resolve, reject) => {
        if (!mailboxItem) {
            reject(new Error('Mailbox item not available'));
            return;
        }
        
        try {
            // In read mode, these are properties, not async methods
            const subject = mailboxItem.subject || 'No subject';
            const from = mailboxItem.from ? mailboxItem.from.emailAddress : 'Unknown';
            const itemId = mailboxItem.itemId || 'Unknown';
            
            // For compose mode or if you need the body, you'd use async methods
            // But for basic info in read mode, properties work fine
            
            // Prepare data to send to Power Automate
            const emailData = {
                subject: subject,
                from: from,
                itemId: itemId,
                triggeredAt: new Date().toISOString(),
                email: Office.context.mailbox.userProfile.emailAddress,
                conversationId: mailboxItem.conversationId || 'Unknown'
            };
            
            console.log('Email data prepared:', emailData);
            resolve(emailData);
            
        } catch (error) {
            console.error('Error getting email data:', error);
            reject(error);
        }
    });
}

function showStatus(message, type) {
    const statusDiv = document.getElementById('statusMessage');
    statusDiv.textContent = message;
    statusDiv.className = 'status ' + type;
    statusDiv.style.display = 'block';
    
    // Auto-hide after 5 seconds for success messages
    if (type === 'success') {
        setTimeout(() => {
            statusDiv.style.display = 'none';
        }, 5000);
    }
}
