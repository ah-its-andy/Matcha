package signer

type RSADescriptorCollection struct {
	source []RSADescriptor
}

func NewRSADescriptorCollection() *RSADescriptorCollection {
	return &RSADescriptorCollection{
		source: make([]RSADescriptor, 0),
	}
}

func (c *RSADescriptorCollection) AnyClientID(clientID string) bool {
	for _, item := range c.source {
		if item.ClientID() == clientID {
			return true
		}
	}
	return false
}

func (c *RSADescriptorCollection) AddOrReplace(item RSADescriptor) {
	if c.AnyClientID(item.ClientID()) {
		c.RemoveClientID(item.ClientID())
	}
	c.source = append(c.source, item)
}

func (c *RSADescriptorCollection) FirstClientID(clientID string) RSADescriptor {
	for _, item := range c.source {
		if item.ClientID() == clientID {
			return item
		}
	}

	return nil
}

func (c *RSADescriptorCollection) RemoveClientID(clientID string) {
	src := make([]RSADescriptor, 0)
	for _, item := range c.source {
		if item.ClientID() != clientID {
			src = append(src, item)
		}
	}
	c.source = src
}
